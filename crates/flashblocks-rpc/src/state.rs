use crate::metrics::Metrics;
use crate::pending_blocks::{PendingBlocks, PendingBlocksBuilder};
use crate::rpc::{FlashblocksAPI, PendingBlocksAPI};
use crate::subscription::{Flashblock, FlashblocksReceiver};
use alloy_consensus::transaction::{Recovered, SignerRecoverable, TransactionMeta};
use alloy_consensus::{Header, TxReceipt};
use alloy_eips::BlockNumberOrTag;
use alloy_primitives::map::foldhash::HashMap;
use alloy_primitives::{Address, BlockNumber, Bytes, Sealable, B256, U256};
use alloy_rpc_types::{TransactionTrait, Withdrawal};
use alloy_rpc_types_engine::{ExecutionPayloadV1, ExecutionPayloadV2, ExecutionPayloadV3};
use alloy_rpc_types_eth::state::StateOverride;
use alloy_rpc_types_eth::{Filter, Log};
use arc_swap::{ArcSwapOption, Guard};
use eyre::eyre;
use op_alloy_consensus::OpTxEnvelope;
use op_alloy_network::{Optimism, TransactionResponse};
use op_alloy_rpc_types::Transaction;
use reth::chainspec::{ChainSpecProvider, EthChainSpec};
use reth::providers::{BlockReaderIdExt, StateProviderFactory};
use reth::revm::context::result::ResultAndState;
use reth::revm::database::StateProviderDatabase;
use reth::revm::db::CacheDB;
use reth::revm::{DatabaseCommit, State};
use reth_evm::{ConfigureEvm, Evm};
use reth_optimism_chainspec::OpHardforks;
use reth_optimism_evm::{OpEvmConfig, OpNextBlockEnvAttributes};
use reth_optimism_primitives::{DepositReceipt, OpBlock, OpPrimitives};
use reth_optimism_rpc::OpReceiptBuilder;
use reth_primitives::RecoveredBlock;
use reth_rpc_convert::{transaction::ConvertReceiptInput, RpcTransaction};
use reth_rpc_eth_api::{RpcBlock, RpcReceipt};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast::{self, Sender};
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

// Buffer 4s of flashblocks for flashblock_sender
const BUFFER_SIZE: usize = 20;

enum StateUpdate {
    Canonical(RecoveredBlock<OpBlock>),
    Flashblock(Flashblock),
}

#[derive(Debug, Clone)]
pub struct FlashblocksState<Client> {
    pending_blocks: Arc<ArcSwapOption<PendingBlocks>>,
    queue: mpsc::UnboundedSender<StateUpdate>,
    flashblock_sender: Sender<Arc<PendingBlocks>>,
    state_processor: StateProcessor<Client>,
}

impl<Client> FlashblocksState<Client>
where
    Client: StateProviderFactory
        + ChainSpecProvider<ChainSpec: EthChainSpec<Header = Header> + OpHardforks>
        + BlockReaderIdExt<Header = Header>
        + Clone
        + 'static,
{
    pub fn new(client: Client) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<StateUpdate>();
        let pending_blocks: Arc<ArcSwapOption<PendingBlocks>> = Arc::new(ArcSwapOption::new(None));
        let (flashblock_sender, _) = broadcast::channel(BUFFER_SIZE);
        let state_processor = StateProcessor::new(
            client,
            pending_blocks.clone(),
            Arc::new(Mutex::new(rx)),
            flashblock_sender.clone(),
        );

        Self {
            pending_blocks,
            queue: tx,
            flashblock_sender,
            state_processor,
        }
    }

    pub fn start(&self) {
        let sp = self.state_processor.clone();
        tokio::spawn(async move {
            sp.start().await;
        });
    }

    pub fn on_canonical_block_received(&self, block: &RecoveredBlock<OpBlock>) {
        match self.queue.send(StateUpdate::Canonical(block.clone())) {
            Ok(_) => {
                info!(
                    message = "added canonical block to processing queue",
                    block_number = block.number
                )
            }
            Err(e) => {
                error!(message = "could not add canonical block to processing queue", block_number = block.number, error = %e);
            }
        }
    }
}

impl<Client> FlashblocksReceiver for FlashblocksState<Client> {
    fn on_flashblock_received(&self, flashblock: Flashblock) {
        match self.queue.send(StateUpdate::Flashblock(flashblock.clone())) {
            Ok(_) => {
                info!(
                    message = "added flashblock to processing queue",
                    block_number = flashblock.metadata.block_number,
                    flashblock_index = flashblock.index
                );
            }
            Err(e) => {
                error!(message = "could not add flashblock to processing queue", block_number = flashblock.metadata.block_number, flashblock_index = flashblock.index, error = %e);
            }
        }
    }
}

impl<Client> FlashblocksAPI for FlashblocksState<Client> {
    fn get_pending_blocks(&self) -> Guard<Option<Arc<PendingBlocks>>> {
        self.pending_blocks.load()
    }

    fn subscribe_to_flashblocks(&self) -> broadcast::Receiver<Arc<PendingBlocks>> {
        self.flashblock_sender.subscribe()
    }
}

impl PendingBlocksAPI for Guard<Option<Arc<PendingBlocks>>> {
    fn get_canonical_block_number(&self) -> BlockNumberOrTag {
        self.as_ref()
            .map(|pb| pb.canonical_block_number())
            .unwrap_or(BlockNumberOrTag::Latest)
    }

    fn get_transaction_count(&self, address: Address) -> U256 {
        self.as_ref()
            .map(|pb| pb.get_transaction_count(address))
            .unwrap_or_else(|| U256::from(0))
    }

    fn get_block(&self, full: bool) -> Option<RpcBlock<Optimism>> {
        self.as_ref().map(|pb| pb.get_latest_block(full))
    }

    fn get_transaction_receipt(
        &self,
        tx_hash: alloy_primitives::TxHash,
    ) -> Option<RpcReceipt<Optimism>> {
        self.as_ref().and_then(|pb| pb.get_receipt(tx_hash))
    }

    fn get_transaction_by_hash(
        &self,
        tx_hash: alloy_primitives::TxHash,
    ) -> Option<RpcTransaction<Optimism>> {
        self.as_ref()
            .and_then(|pb| pb.get_transaction_by_hash(tx_hash))
    }

    fn get_balance(&self, address: Address) -> Option<U256> {
        self.as_ref().and_then(|pb| pb.get_balance(address))
    }

    fn get_state_overrides(&self) -> Option<StateOverride> {
        self.as_ref()
            .map(|pb| pb.get_state_overrides())
            .unwrap_or_default()
    }

    fn get_pending_logs(&self, filter: &Filter) -> Vec<Log> {
        self.as_ref()
            .map(|pb| pb.get_pending_logs(filter))
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
struct StateProcessor<Client> {
    rx: Arc<Mutex<UnboundedReceiver<StateUpdate>>>,
    pending_blocks: Arc<ArcSwapOption<PendingBlocks>>,
    metrics: Metrics,
    client: Client,
    sender: Sender<Arc<PendingBlocks>>,
}

impl<Client> StateProcessor<Client>
where
    Client: StateProviderFactory
        + ChainSpecProvider<ChainSpec: EthChainSpec<Header = Header> + OpHardforks>
        + BlockReaderIdExt<Header = Header>
        + Clone
        + 'static,
{
    fn new(
        client: Client,
        pending_blocks: Arc<ArcSwapOption<PendingBlocks>>,
        rx: Arc<Mutex<UnboundedReceiver<StateUpdate>>>,
        sender: Sender<Arc<PendingBlocks>>,
    ) -> Self {
        Self {
            metrics: Metrics::default(),
            pending_blocks,
            client,
            rx,
            sender,
        }
    }

    async fn start(&self) {
        while let Some(update) = self.rx.lock().await.recv().await {
            let prev_pending_blocks = self.pending_blocks.load_full();
            match update {
                StateUpdate::Canonical(block) => {
                    debug!(
                        message = "processing canonical block",
                        block_number = block.number
                    );
                    match self.process_canonical_block(prev_pending_blocks, &block) {
                        Ok(new_pending_blocks) => {
                            self.pending_blocks.swap(new_pending_blocks);
                        }
                        Err(e) => {
                            error!(message = "could not process canonical block", error = %e);
                        }
                    }
                }
                StateUpdate::Flashblock(flashblock) => {
                    let start_time = Instant::now();
                    debug!(
                        message = "processing flashblock",
                        block_number = flashblock.metadata.block_number,
                        flashblock_index = flashblock.index
                    );
                    match self.process_flashblock(prev_pending_blocks, flashblock) {
                        Ok(new_pending_blocks) => {
                            if new_pending_blocks.is_some() {
                                _ = self.sender.send(new_pending_blocks.clone().unwrap())
                            }

                            self.pending_blocks.swap(new_pending_blocks);
                            self.metrics
                                .block_processing_duration
                                .record(start_time.elapsed());
                        }
                        Err(e) => {
                            error!(message = "could not process Flashblock", error = %e);
                            self.metrics.block_processing_error.increment(1);
                        }
                    }
                }
            }
        }
    }

    fn process_canonical_block(
        &self,
        prev_pending_blocks: Option<Arc<PendingBlocks>>,
        block: &RecoveredBlock<OpBlock>,
    ) -> eyre::Result<Option<Arc<PendingBlocks>>> {
        match &prev_pending_blocks {
            Some(pending_blocks) => {
                let mut flashblocks = pending_blocks.get_flashblocks();
                let num_flashblocks_for_canon = flashblocks
                    .iter()
                    .filter(|fb| fb.metadata.block_number == block.number)
                    .count();
                self.metrics
                    .flashblocks_in_block
                    .record(num_flashblocks_for_canon as f64);

                if pending_blocks.latest_block_number() <= block.number {
                    self.metrics.pending_clear_catchup.increment(1);
                    self.metrics
                        .pending_snapshot_height
                        .set(pending_blocks.latest_block_number() as f64);
                    self.metrics
                        .pending_snapshot_fb_index
                        .set(pending_blocks.latest_flashblock_index() as f64);

                    Ok(None)
                } else {
                    // If we had a reorg, we need to reset all flashblocks state
                    let tracked_txns = pending_blocks.get_transactions_for_block(block.number);
                    let tracked_txn_hashes: HashSet<_> =
                        tracked_txns.iter().map(|tx| tx.tx_hash()).collect();
                    let block_txn_hashes: HashSet<_> =
                        block.body().transactions().map(|tx| tx.tx_hash()).collect();

                    flashblocks
                        .retain(|flashblock| flashblock.metadata.block_number > block.number);

                    if tracked_txn_hashes.len() != block_txn_hashes.len()
                        || tracked_txn_hashes != block_txn_hashes
                    {
                        debug!(
                            message = "reorg detected, recomputing pending flashblocks going ahead of reorg",
                            latest_pending_block = pending_blocks.latest_block_number(),
                            canonical_block = block.number,
                            tracked_txn_hashes_len = tracked_txn_hashes.len(),
                            block_txn_hashes_len = block_txn_hashes.len(),
                            tracked_txn_hashes = ?tracked_txn_hashes,
                            block_txn_hashes = ?block_txn_hashes,
                        );
                        self.metrics.pending_clear_reorg.increment(1);

                        // If there is a reorg, we re-process all future flashblocks without reusing the existing pending state
                        return self.build_pending_state(None, &flashblocks);
                    }

                    // If no reorg, we can continue building on top of the existing pending state
                    self.build_pending_state(prev_pending_blocks, &flashblocks)
                }
            }
            None => {
                debug!(message = "no pending state to update with canonical block, skipping");
                Ok(None)
            }
        }
    }

    fn process_flashblock(
        &self,
        prev_pending_blocks: Option<Arc<PendingBlocks>>,
        flashblock: Flashblock,
    ) -> eyre::Result<Option<Arc<PendingBlocks>>> {
        match &prev_pending_blocks {
            Some(pending_blocks) => {
                if self.is_next_flashblock(pending_blocks, &flashblock) {
                    // We have received the next flashblock for the current block
                    // or the first flashblock for the next block
                    let mut flashblocks = pending_blocks.get_flashblocks();
                    flashblocks.push(flashblock);
                    self.build_pending_state(prev_pending_blocks, &flashblocks)
                } else if pending_blocks.latest_block_number() != flashblock.metadata.block_number {
                    // We have received a non-zero flashblock for a new block
                    self.metrics.unexpected_block_order.increment(1);
                    error!(
                        message = "Received non-zero index Flashblock for new block, zeroing Flashblocks until we receive a base Flashblock",
                        curr_block = %pending_blocks.latest_block_number(),
                        new_block = %flashblock.metadata.block_number,
                    );
                    Ok(None)
                } else if pending_blocks.latest_flashblock_index() == flashblock.index {
                    // We have received a duplicate flashblock for the current block
                    self.metrics.unexpected_block_order.increment(1);
                    warn!(
                        message = "Received duplicate Flashblock for current block, ignoring",
                        curr_block = %pending_blocks.latest_block_number(),
                        flashblock_index = %flashblock.index,
                    );
                    Ok(prev_pending_blocks)
                } else {
                    // We have received a non-sequential flashblock for the current block
                    self.metrics.unexpected_block_order.increment(1);

                    error!(
                        message = "Received non-sequential Flashblock for current block, zeroing Flashblocks until we receive a base Flashblock",
                        curr_block = %pending_blocks.latest_block_number(),
                        new_block = %flashblock.metadata.block_number,
                    );

                    Ok(None)
                }
            }
            None => {
                if flashblock.index == 0 {
                    self.build_pending_state(None, &vec![flashblock])
                } else {
                    info!(message = "waiting for first Flashblock");
                    Ok(None)
                }
            }
        }
    }

    fn build_pending_state(
        &self,
        prev_pending_blocks: Option<Arc<PendingBlocks>>,
        flashblocks: &Vec<Flashblock>,
    ) -> eyre::Result<Option<Arc<PendingBlocks>>> {
        // BTreeMap guarantees ascending order of keys while iterating
        let mut flashblocks_per_block = BTreeMap::<BlockNumber, Vec<&Flashblock>>::new();
        for flashblock in flashblocks {
            flashblocks_per_block
                .entry(flashblock.metadata.block_number)
                .or_default()
                .push(flashblock);
        }

        let earliest_block_number = flashblocks_per_block.keys().min().unwrap();
        let canonical_block = earliest_block_number - 1;
        let mut last_block_header = self.client.header_by_number(canonical_block)?.ok_or(eyre!(
            "Failed to extract header for canonical block number {}. This is okay if your node is not fully synced to tip yet.",
            canonical_block
        ))?;

        let evm_config = OpEvmConfig::optimism(self.client.chain_spec());

        let state_provider = self
            .client
            .state_by_block_number_or_tag(BlockNumberOrTag::Number(canonical_block))?;
        let state_provider_db = StateProviderDatabase::new(state_provider);
        let state = State::builder()
            .with_database(state_provider_db)
            .with_bundle_update()
            .build();
        let mut pending_blocks_builder = PendingBlocksBuilder::new();

        let mut db = match &prev_pending_blocks {
            Some(pending_blocks) => CacheDB {
                cache: pending_blocks.get_db_cache(),
                db: state,
            },
            None => CacheDB::new(state),
        };
        let mut state_overrides = match &prev_pending_blocks {
            Some(pending_blocks) => pending_blocks.get_state_overrides().unwrap_or_default(),
            None => StateOverride::default(),
        };

        for (_block_number, flashblocks) in flashblocks_per_block {
            let base = flashblocks
                .first()
                .ok_or(eyre!("cannot build a pending block from no flashblocks"))?
                .base
                .clone()
                .ok_or(eyre!("first flashblock does not contain a base"))?;

            let latest_flashblock = flashblocks
                .last()
                .cloned()
                .ok_or(eyre!("cannot build a pending block from no flashblocks"))?;

            let transactions: Vec<Bytes> = flashblocks
                .iter()
                .flat_map(|flashblock| flashblock.diff.transactions.clone())
                .collect();

            let withdrawals: Vec<Withdrawal> = flashblocks
                .iter()
                .flat_map(|flashblock| flashblock.diff.withdrawals.clone())
                .collect();

            let receipt_by_hash = flashblocks
                .iter()
                .map(|flashblock| flashblock.metadata.receipts.clone())
                .fold(HashMap::default(), |mut acc, receipts| {
                    acc.extend(receipts);
                    acc
                });

            let updated_balances = flashblocks
                .iter()
                .map(|flashblock| flashblock.metadata.new_account_balances.clone())
                .fold(HashMap::default(), |mut acc, balances| {
                    acc.extend(balances);
                    acc
                });

            pending_blocks_builder.with_flashblocks(
                flashblocks
                    .iter()
                    .map(|&x| x.clone())
                    .collect::<Vec<Flashblock>>(),
            );

            let execution_payload: ExecutionPayloadV3 = ExecutionPayloadV3 {
                blob_gas_used: 0,
                excess_blob_gas: 0,
                payload_inner: ExecutionPayloadV2 {
                    withdrawals,
                    payload_inner: ExecutionPayloadV1 {
                        parent_hash: base.parent_hash,
                        fee_recipient: base.fee_recipient,
                        state_root: latest_flashblock.diff.state_root,
                        receipts_root: latest_flashblock.diff.receipts_root,
                        logs_bloom: latest_flashblock.diff.logs_bloom,
                        prev_randao: base.prev_randao,
                        block_number: base.block_number,
                        gas_limit: base.gas_limit,
                        gas_used: latest_flashblock.diff.gas_used,
                        timestamp: base.timestamp,
                        extra_data: base.extra_data.clone(),
                        base_fee_per_gas: base.base_fee_per_gas,
                        block_hash: latest_flashblock.diff.block_hash,
                        transactions,
                    },
                },
            };

            let block: OpBlock = execution_payload.try_into_block()?;
            let mut l1_block_info = reth_optimism_evm::extract_l1_info(&block.body)?;
            let header = block.header.clone().seal_slow();
            pending_blocks_builder.with_header(header.clone());

            let block_env_attributes = OpNextBlockEnvAttributes {
                timestamp: base.timestamp,
                suggested_fee_recipient: base.fee_recipient,
                prev_randao: base.prev_randao,
                gas_limit: base.gas_limit,
                parent_beacon_block_root: Some(base.parent_beacon_block_root),
                extra_data: base.extra_data.clone(),
            };

            let evm_env = evm_config.next_evm_env(&last_block_header, &block_env_attributes)?;
            let mut evm = evm_config.evm_with_env(db, evm_env);

            let mut gas_used = 0;
            let mut next_log_index = 0;

            for (idx, transaction) in block.body.transactions.iter().enumerate() {
                let sender = match transaction.recover_signer() {
                    Ok(signer) => signer,
                    Err(err) => return Err(err.into()),
                };
                pending_blocks_builder.increment_nonce(sender);

                let receipt = receipt_by_hash
                    .get(&transaction.tx_hash())
                    .cloned()
                    .ok_or(eyre!("missing receipt for {:?}", transaction.tx_hash()))?;

                let recovered_transaction = Recovered::new_unchecked(transaction.clone(), sender);
                let envelope = recovered_transaction.clone().convert::<OpTxEnvelope>();

                // Build Transaction
                let (deposit_receipt_version, deposit_nonce) = if transaction.is_deposit() {
                    let deposit_receipt = receipt
                        .as_deposit_receipt()
                        .ok_or(eyre!("deposit transaction, non deposit receipt"))?;

                    (
                        deposit_receipt.deposit_receipt_version,
                        deposit_receipt.deposit_nonce,
                    )
                } else {
                    (None, None)
                };

                let effective_gas_price = if transaction.is_deposit() {
                    0
                } else {
                    block
                        .base_fee_per_gas
                        .map(|base_fee| {
                            transaction
                                .effective_tip_per_gas(base_fee)
                                .unwrap_or_default()
                                + base_fee as u128
                        })
                        .unwrap_or_else(|| transaction.max_fee_per_gas())
                };

                let rpc_txn = Transaction {
                    inner: alloy_rpc_types_eth::Transaction {
                        inner: envelope,
                        block_hash: Some(header.hash()),
                        block_number: Some(base.block_number),
                        transaction_index: Some(idx as u64),
                        effective_gas_price: Some(effective_gas_price),
                    },
                    deposit_nonce,
                    deposit_receipt_version,
                };

                pending_blocks_builder.with_transaction(rpc_txn);

                // Receipt Generation
                let meta = TransactionMeta {
                    tx_hash: transaction.tx_hash(),
                    index: idx as u64,
                    block_hash: header.hash(),
                    block_number: block.number,
                    base_fee: block.base_fee_per_gas,
                    excess_blob_gas: block.excess_blob_gas,
                    timestamp: block.timestamp,
                };

                let input: ConvertReceiptInput<'_, OpPrimitives> = ConvertReceiptInput {
                    receipt: receipt.clone(),
                    tx: Recovered::new_unchecked(transaction, sender),
                    gas_used: receipt.cumulative_gas_used() - gas_used,
                    next_log_index,
                    meta,
                };

                let op_receipt = OpReceiptBuilder::new(
                    self.client.chain_spec().as_ref(),
                    input,
                    &mut l1_block_info,
                )?
                .build();

                pending_blocks_builder.with_receipt(transaction.tx_hash(), op_receipt);
                gas_used = receipt.cumulative_gas_used();
                next_log_index += receipt.logs().len();

                let mut should_execute_transaction = false;
                match &prev_pending_blocks {
                    Some(pending_blocks) => {
                        match pending_blocks.get_transaction_state(transaction.tx_hash()) {
                            Some(state) => {
                                pending_blocks_builder
                                    .with_transaction_state(transaction.tx_hash(), state);
                            }
                            None => {
                                should_execute_transaction = true;
                            }
                        }
                    }
                    None => {
                        should_execute_transaction = true;
                    }
                }

                if should_execute_transaction {
                    match evm.transact(recovered_transaction) {
                        Ok(ResultAndState { state, .. }) => {
                            for (addr, acc) in &state {
                                let existing_override =
                                    state_overrides.entry(*addr).or_insert(Default::default());
                                existing_override.balance = Some(acc.info.balance);
                                existing_override.nonce = Some(acc.info.nonce);
                                existing_override.code =
                                    acc.info.code.clone().map(|code| code.bytes());

                                let existing = existing_override
                                    .state_diff
                                    .get_or_insert(Default::default());
                                let changed_slots = acc.storage.iter().map(|(&key, slot)| {
                                    (B256::from(key), B256::from(slot.present_value))
                                });

                                existing.extend(changed_slots);
                            }
                            pending_blocks_builder
                                .with_transaction_state(transaction.tx_hash(), state.clone());
                            evm.db_mut().commit(state);
                        }
                        Err(e) => {
                            return Err(eyre!(
                                "failed to execute transaction: {:?} tx_hash: {:?} sender: {:?}",
                                e,
                                transaction.tx_hash(),
                                sender
                            ));
                        }
                    }
                }
            }

            for (address, balance) in updated_balances {
                pending_blocks_builder.with_account_balance(address, balance);
            }

            db = evm.into_db();
            last_block_header = block.header.clone();
        }

        pending_blocks_builder.with_db_cache(db.cache);
        pending_blocks_builder.with_state_overrides(state_overrides);
        Ok(Some(Arc::new(pending_blocks_builder.build()?)))
    }

    fn is_next_flashblock(
        &self,
        pending_blocks: &Arc<PendingBlocks>,
        flashblock: &Flashblock,
    ) -> bool {
        let is_next_of_block = flashblock.metadata.block_number
            == pending_blocks.latest_block_number()
            && flashblock.index == pending_blocks.latest_flashblock_index() + 1;
        let is_first_of_next_block = flashblock.metadata.block_number
            == pending_blocks.latest_block_number() + 1
            && flashblock.index == 0;

        is_next_of_block || is_first_of_next_block
    }
}

#[cfg(test)]
mod tests {
    use crate::rpc::{FlashblocksAPI, PendingBlocksAPI};
    use crate::state::FlashblocksState;
    use crate::subscription::{Flashblock, FlashblocksReceiver, Metadata};
    use crate::test_utils::create_test_provider_factory;
    use alloy_primitives::{b256, bytes};

    // Test constants
    const BLOCK_INFO_TXN: Bytes = bytes!("0x7ef90104a06c0c775b6b492bab9d7e81abdf27f77cafb698551226455a82f559e0f93fea3794deaddeaddeaddeaddeaddeaddeaddeaddead00019442000000000000000000000000000000000000158080830f424080b8b0098999be000008dd00101c1200000000000000020000000068869d6300000000015f277f000000000000000000000000000000000000000000000000000000000d42ac290000000000000000000000000000000000000000000000000000000000000001abf52777e63959936b1bf633a2a643f0da38d63deffe49452fed1bf8a44975d50000000000000000000000005050f69a9786f081509234f1a7f4684b5e5b76c9000000000000000000000000");
    const BLOCK_INFO_TXN_HASH: B256 =
        b256!("0xba56c8b0deb460ff070f8fca8e2ee01e51a3db27841cc862fdd94cc1a47662b6");
    use alloy_consensus::crypto::secp256k1::public_key_to_address;
    use alloy_consensus::{BlockHeader, Receipt};
    use alloy_consensus::{Header, Transaction};
    use alloy_eips::{BlockHashOrNumber, Decodable2718, Encodable2718};
    use alloy_genesis::GenesisAccount;
    use alloy_primitives::map::foldhash::HashMap;
    use alloy_primitives::{Address, BlockNumber, Bytes, B256, U256};
    use alloy_provider::network::BlockResponse;
    use alloy_rpc_types_engine::PayloadId;
    use op_alloy_consensus::OpDepositReceipt;
    use reth::builder::NodeTypesWithDBAdapter;
    use reth::chainspec::EthChainSpec;
    use reth::providers::{AccountReader, BlockNumReader, BlockReader};
    use reth::revm::database::StateProviderDatabase;
    use reth::transaction_pool::test_utils::TransactionBuilder;
    use reth_db::{test_utils::TempDatabase, DatabaseEnv};
    use reth_evm::execute::Executor;
    use reth_evm::ConfigureEvm;
    use reth_optimism_chainspec::{OpChainSpecBuilder, BASE_MAINNET};
    use reth_optimism_evm::OpEvmConfig;
    use reth_optimism_node::OpNode;
    use reth_optimism_primitives::{OpBlock, OpBlockBody, OpReceipt, OpTransactionSigned};
    use reth_primitives_traits::{Account, Block, RecoveredBlock, SealedHeader};
    use reth_provider::providers::BlockchainProvider;
    use reth_provider::{
        BlockWriter, ChainSpecProvider, ExecutionOutcome, LatestStateProviderRef, ProviderFactory,
        StateProviderFactory,
    };
    use rollup_boost::{ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1};
    use std::sync::Arc;
    use std::sync::Once;
    use std::time::Duration;
    use tokio::time::sleep;

    static INIT_LOGGING: Once = Once::new();
    fn init_logging_once() {
        INIT_LOGGING.call_once(|| {
            if std::env::var("RUST_LOG").is_err() {
                std::env::set_var(
                    "RUST_LOG",
                    "warn,reth_tasks=off,base_reth_flashblocks_rpc::state=off",
                );
            }
            reth_tracing::init_test_tracing();
        });
    }

    // The amount of time to wait (in milliseconds) after sending a new flashblock or canonical block
    // so it can be processed by the state processor
    const SLEEP_TIME: u64 = 10;

    #[derive(Eq, PartialEq, Debug, Hash, Clone, Copy)]
    enum User {
        Alice,
        Bob,
        Charlie,
    }

    type NodeTypes = NodeTypesWithDBAdapter<OpNode, Arc<TempDatabase<DatabaseEnv>>>;

    #[derive(Debug, Clone)]
    struct TestHarness {
        flashblocks: FlashblocksState<BlockchainProvider<NodeTypes>>,
        provider: BlockchainProvider<NodeTypes>,
        factory: ProviderFactory<NodeTypes>,
        user_to_address: HashMap<User, Address>,
        user_to_private_key: HashMap<User, B256>,
    }

    impl TestHarness {
        fn address(&self, u: User) -> Address {
            assert!(self.user_to_address.contains_key(&u));
            self.user_to_address[&u]
        }

        fn signer(&self, u: User) -> B256 {
            assert!(self.user_to_private_key.contains_key(&u));
            self.user_to_private_key[&u]
        }

        fn current_canonical_block(&self) -> RecoveredBlock<OpBlock> {
            let latest_block_num = self
                .provider
                .last_block_number()
                .expect("should be a latest block");

            self.provider
                .block(BlockHashOrNumber::Number(latest_block_num))
                .expect("able to load block")
                .expect("block exists")
                .try_into_recovered()
                .expect("able to recover block")
        }

        fn account_state(&self, u: User) -> Account {
            let basic_account = self
                .provider
                .basic_account(&self.address(u))
                .expect("can lookup account state")
                .expect("should be existing account state");

            let nonce = self
                .flashblocks
                .get_pending_blocks()
                .get_transaction_count(self.address(u))
                .to::<u64>();
            let balance = self
                .flashblocks
                .get_pending_blocks()
                .get_balance(self.address(u))
                .unwrap_or(basic_account.balance);

            Account {
                nonce: nonce + basic_account.nonce,
                balance,
                bytecode_hash: basic_account.bytecode_hash,
            }
        }

        fn build_transaction_to_send_eth(
            &self,
            from: User,
            to: User,
            amount: u128,
        ) -> OpTransactionSigned {
            let txn = TransactionBuilder::default()
                .signer(self.signer(from))
                .chain_id(self.provider.chain_spec().chain_id())
                .to(self.address(to))
                .nonce(self.account_state(from).nonce)
                .value(amount)
                .gas_limit(21_000)
                .max_fee_per_gas(200)
                .into_eip1559()
                .as_eip1559()
                .unwrap()
                .clone();

            OpTransactionSigned::Eip1559(txn)
        }

        fn build_transaction_to_send_eth_with_nonce(
            &self,
            from: User,
            to: User,
            amount: u128,
            nonce: u64,
        ) -> OpTransactionSigned {
            let txn = TransactionBuilder::default()
                .signer(self.signer(from))
                .chain_id(self.provider.chain_spec().chain_id())
                .to(self.address(to))
                .nonce(nonce)
                .value(amount)
                .gas_limit(21_000)
                .max_fee_per_gas(200)
                .into_eip1559()
                .as_eip1559()
                .unwrap()
                .clone();

            OpTransactionSigned::Eip1559(txn)
        }

        async fn send_flashblock(&self, flashblock: Flashblock) {
            self.flashblocks.on_flashblock_received(flashblock);
            sleep(Duration::from_millis(SLEEP_TIME)).await;
        }

        async fn new_canonical_block_without_processing(
            &mut self,
            mut user_transactions: Vec<OpTransactionSigned>,
        ) -> RecoveredBlock<OpBlock> {
            let current_tip = self.current_canonical_block();

            let deposit_transaction =
                OpTransactionSigned::decode_2718_exact(BLOCK_INFO_TXN.iter().as_slice()).unwrap();

            let mut transactions: Vec<OpTransactionSigned> = vec![deposit_transaction];
            transactions.append(&mut user_transactions);

            let block = OpBlock::new_sealed(
                SealedHeader::new_unhashed(Header {
                    parent_beacon_block_root: Some(current_tip.hash()),
                    parent_hash: current_tip.hash(),
                    number: current_tip.number() + 1,
                    timestamp: current_tip.header().timestamp() + 2,
                    gas_limit: current_tip.header().gas_limit(),
                    ..Header::default()
                }),
                OpBlockBody {
                    transactions,
                    ommers: vec![],
                    withdrawals: None,
                },
            )
            .try_recover()
            .expect("able to recover block");

            let provider = self.factory.provider().unwrap();

            // Execute the block to produce a block execution output
            let mut block_execution_output = OpEvmConfig::optimism(self.provider.chain_spec())
                .batch_executor(StateProviderDatabase::new(LatestStateProviderRef::new(
                    &provider,
                )))
                .execute(&block)
                .unwrap();

            block_execution_output.state.reverts.sort();

            let execution_outcome = ExecutionOutcome {
                bundle: block_execution_output.state.clone(),
                receipts: vec![block_execution_output.receipts.clone()],
                first_block: block.number,
                requests: vec![block_execution_output.requests.clone()],
            };

            // Commit the block's execution outcome to the database
            let provider_rw = self.factory.provider_rw().unwrap();
            provider_rw
                .append_blocks_with_state(
                    vec![block.clone()],
                    &execution_outcome,
                    Default::default(),
                )
                .unwrap();
            provider_rw.commit().unwrap();

            block
        }

        async fn new_canonical_block(&mut self, user_transactions: Vec<OpTransactionSigned>) {
            let block = self
                .new_canonical_block_without_processing(user_transactions)
                .await;
            self.flashblocks.on_canonical_block_received(&block);
            sleep(Duration::from_millis(SLEEP_TIME)).await;
        }

        fn new() -> Self {
            let keys = reth_testing_utils::generators::generate_keys(&mut rand::rng(), 3);
            let alice_signer = keys[0];
            let bob_signer = keys[1];
            let charli_signer = keys[2];

            let alice = public_key_to_address(alice_signer.public_key());
            let bob = public_key_to_address(bob_signer.public_key());
            let charlie = public_key_to_address(charli_signer.public_key());

            let items = vec![
                (
                    alice,
                    GenesisAccount::default().with_balance(U256::from(100_000_000)),
                ),
                (
                    bob,
                    GenesisAccount::default().with_balance(U256::from(100_000_000)),
                ),
                (
                    charlie,
                    GenesisAccount::default().with_balance(U256::from(100_000_000)),
                ),
            ];

            let genesis = BASE_MAINNET
                .genesis
                .clone()
                .extend_accounts(items)
                .with_gas_limit(100_000_000);

            let chain_spec = OpChainSpecBuilder::base_mainnet()
                .genesis(genesis)
                .isthmus_activated()
                .build();

            let factory = create_test_provider_factory::<OpNode>(Arc::new(chain_spec));
            assert!(reth_db_common::init::init_genesis(&factory).is_ok());

            let provider =
                BlockchainProvider::new(factory.clone()).expect("able to setup provider");

            let block = provider
                .block(BlockHashOrNumber::Number(0))
                .expect("able to load block")
                .expect("block exists")
                .try_into_recovered()
                .expect("able to recover block");

            let flashblocks = FlashblocksState::new(provider.clone());
            flashblocks.start();

            flashblocks.on_canonical_block_received(&block);

            Self {
                factory,
                flashblocks,
                provider,
                user_to_address: {
                    let mut res = HashMap::default();
                    res.insert(User::Alice, alice);
                    res.insert(User::Bob, bob);
                    res.insert(User::Charlie, charlie);
                    res
                },
                user_to_private_key: {
                    let mut res = HashMap::default();
                    res.insert(User::Alice, alice_signer.secret_bytes().into());
                    res.insert(User::Bob, bob_signer.secret_bytes().into());
                    res.insert(User::Charlie, charli_signer.secret_bytes().into());
                    res
                },
            }
        }
    }

    struct FlashblockBuilder {
        transactions: Vec<Bytes>,
        receipts: HashMap<B256, OpReceipt>,
        harness: TestHarness,
        canonical_block_number: Option<BlockNumber>,
        index: u64,
    }

    impl FlashblockBuilder {
        pub fn new_base(harness: &TestHarness) -> Self {
            Self {
                canonical_block_number: None,
                transactions: vec![BLOCK_INFO_TXN.clone()],
                receipts: {
                    let mut receipts = alloy_primitives::map::HashMap::default();
                    receipts.insert(
                        BLOCK_INFO_TXN_HASH,
                        OpReceipt::Deposit(OpDepositReceipt {
                            inner: Receipt {
                                status: true.into(),
                                cumulative_gas_used: 10000,
                                logs: vec![],
                            },
                            deposit_nonce: Some(4012991u64),
                            deposit_receipt_version: None,
                        }),
                    );
                    receipts
                },
                index: 0,
                harness: harness.clone(),
            }
        }
        pub fn new(harness: &TestHarness, index: u64) -> Self {
            Self {
                canonical_block_number: None,
                transactions: Vec::new(),
                receipts: HashMap::default(),
                harness: harness.clone(),
                index,
            }
        }

        pub fn with_receipts(&mut self, receipts: HashMap<B256, OpReceipt>) -> &mut Self {
            self.receipts = receipts;
            self
        }

        pub fn with_transactions(&mut self, transactions: Vec<OpTransactionSigned>) -> &mut Self {
            assert_ne!(self.index, 0, "Cannot set txns for initial flashblock");
            self.transactions.clear();

            let mut cumulative_gas_used = 0;
            for txn in transactions.iter() {
                cumulative_gas_used += txn.gas_limit();
                self.transactions.push(txn.encoded_2718().into());
                self.receipts.insert(
                    *txn.hash(),
                    OpReceipt::Eip1559(Receipt {
                        status: true.into(),
                        cumulative_gas_used,
                        logs: vec![],
                    }),
                );
            }
            self
        }

        pub fn with_canonical_block_number(&mut self, num: BlockNumber) -> &mut Self {
            self.canonical_block_number = Some(num);
            self
        }

        pub fn build(&self) -> Flashblock {
            let current_block = self.harness.current_canonical_block();
            let canonical_block_num = self
                .canonical_block_number
                .unwrap_or_else(|| current_block.number)
                + 1;

            let base = if self.index == 0 {
                Some(ExecutionPayloadBaseV1 {
                    parent_beacon_block_root: current_block.hash(),
                    parent_hash: current_block.hash(),
                    fee_recipient: Address::random(),
                    prev_randao: B256::random(),
                    block_number: canonical_block_num,
                    gas_limit: current_block.gas_limit,
                    timestamp: current_block.timestamp + 2,
                    extra_data: Bytes::new(),
                    base_fee_per_gas: U256::from(100),
                })
            } else {
                None
            };

            Flashblock {
                payload_id: PayloadId::default(),
                index: self.index,
                base,
                diff: ExecutionPayloadFlashblockDeltaV1 {
                    state_root: B256::default(),
                    receipts_root: B256::default(),
                    block_hash: B256::default(),
                    gas_used: 0,
                    withdrawals: Vec::new(),
                    logs_bloom: Default::default(),
                    withdrawals_root: Default::default(),
                    transactions: self.transactions.clone(),
                },
                metadata: Metadata {
                    block_number: canonical_block_num,
                    receipts: self.receipts.clone(),
                    new_account_balances: HashMap::default(),
                },
            }
        }
    }

    #[tokio::test]
    async fn test_state_overrides_persisted_across_flashblocks() {
        init_logging_once();
        let test = TestHarness::new();

        test.send_flashblock(FlashblockBuilder::new_base(&test).build())
            .await;
        assert_eq!(
            test.flashblocks
                .get_pending_blocks()
                .get_block(true)
                .expect("block is built")
                .transactions
                .len(),
            1
        );

        assert!(test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .is_some());
        assert!(!test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .unwrap()
            .contains_key(&test.address(User::Alice)));

        test.send_flashblock(
            FlashblockBuilder::new(&test, 1)
                .with_transactions(vec![test.build_transaction_to_send_eth(
                    User::Alice,
                    User::Bob,
                    100_000,
                )])
                .build(),
        )
        .await;

        let pending = test.flashblocks.get_pending_blocks().get_block(true);
        assert!(pending.is_some());
        let pending = pending.unwrap();
        assert_eq!(pending.transactions.len(), 2);

        let overrides = test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .expect("should be set from txn execution");

        assert!(overrides.contains_key(&test.address(User::Alice)));
        assert_eq!(
            overrides
                .get(&test.address(User::Bob))
                .expect("should be set as txn receiver")
                .balance
                .expect("should be changed due to receiving funds"),
            U256::from(100_100_000)
        );

        test.send_flashblock(FlashblockBuilder::new(&test, 2).build())
            .await;

        let overrides = test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .expect("should be set from txn execution in flashblock index 1");

        assert!(overrides.contains_key(&test.address(User::Alice)));
        assert_eq!(
            overrides
                .get(&test.address(User::Bob))
                .expect("should be set as txn receiver")
                .balance
                .expect("should be changed due to receiving funds"),
            U256::from(100_100_000)
        );
    }

    #[tokio::test]
    async fn test_state_overrides_persisted_across_blocks() {
        init_logging_once();
        let test = TestHarness::new();

        let initial_base = FlashblockBuilder::new_base(&test).build();
        let initial_block_number = initial_base.metadata.block_number;
        test.send_flashblock(initial_base).await;
        assert_eq!(
            test.flashblocks
                .get_pending_blocks()
                .get_block(true)
                .expect("block is built")
                .transactions
                .len(),
            1
        );

        assert!(test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .is_some());
        assert!(!test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .unwrap()
            .contains_key(&test.address(User::Alice)));

        test.send_flashblock(
            FlashblockBuilder::new(&test, 1)
                .with_transactions(vec![test.build_transaction_to_send_eth(
                    User::Alice,
                    User::Bob,
                    100_000,
                )])
                .build(),
        )
        .await;

        let pending = test.flashblocks.get_pending_blocks().get_block(true);
        assert!(pending.is_some());
        let pending = pending.unwrap();
        assert_eq!(pending.transactions.len(), 2);

        let overrides = test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .expect("should be set from txn execution");

        assert!(overrides.contains_key(&test.address(User::Alice)));
        assert_eq!(
            overrides
                .get(&test.address(User::Bob))
                .expect("should be set as txn receiver")
                .balance
                .expect("should be changed due to receiving funds"),
            U256::from(100_100_000)
        );

        test.send_flashblock(
            FlashblockBuilder::new_base(&test)
                .with_canonical_block_number(initial_block_number)
                .build(),
        )
        .await;

        assert_eq!(
            test.flashblocks
                .get_pending_blocks()
                .get_block(true)
                .expect("block is built")
                .transactions
                .len(),
            1
        );
        assert_eq!(
            test.flashblocks
                .get_pending_blocks()
                .get_block(true)
                .expect("block is built")
                .header
                .number,
            initial_block_number + 1
        );

        assert!(test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .is_some());
        assert!(test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .unwrap()
            .contains_key(&test.address(User::Alice)));

        test.send_flashblock(
            FlashblockBuilder::new(&test, 1)
                .with_canonical_block_number(initial_block_number)
                .with_transactions(vec![test.build_transaction_to_send_eth(
                    User::Alice,
                    User::Bob,
                    100_000,
                )])
                .build(),
        )
        .await;

        let overrides = test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .expect("should be set from txn execution");

        assert!(overrides.contains_key(&test.address(User::Alice)));
        assert_eq!(
            overrides
                .get(&test.address(User::Bob))
                .expect("should be set as txn receiver")
                .balance
                .expect("should be changed due to receiving funds"),
            U256::from(100_200_000)
        );
    }

    #[tokio::test]
    async fn test_only_current_pending_state_cleared_upon_canonical_block_reorg() {
        init_logging_once();
        let mut test = TestHarness::new();

        test.send_flashblock(FlashblockBuilder::new_base(&test).build())
            .await;
        assert_eq!(
            test.flashblocks
                .get_pending_blocks()
                .get_block(true)
                .expect("block is built")
                .transactions
                .len(),
            1
        );
        assert!(test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .is_some());
        assert!(!test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .unwrap()
            .contains_key(&test.address(User::Alice)));

        test.send_flashblock(
            FlashblockBuilder::new(&test, 1)
                .with_transactions(vec![test.build_transaction_to_send_eth(
                    User::Alice,
                    User::Bob,
                    100_000,
                )])
                .build(),
        )
        .await;
        let pending = test.flashblocks.get_pending_blocks().get_block(true);
        assert!(pending.is_some());
        let pending = pending.unwrap();
        assert_eq!(pending.transactions.len(), 2);

        let overrides = test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .expect("should be set from txn execution");

        assert!(overrides.contains_key(&test.address(User::Alice)));
        assert_eq!(
            overrides
                .get(&test.address(User::Bob))
                .expect("should be set as txn receiver")
                .balance
                .expect("should be changed due to receiving funds"),
            U256::from(100_100_000)
        );

        test.send_flashblock(
            FlashblockBuilder::new_base(&test)
                .with_canonical_block_number(1)
                .build(),
        )
        .await;
        test.send_flashblock(
            FlashblockBuilder::new(&test, 1)
                .with_canonical_block_number(1)
                .with_transactions(vec![test.build_transaction_to_send_eth(
                    User::Alice,
                    User::Bob,
                    100_000,
                )])
                .build(),
        )
        .await;
        let pending = test.flashblocks.get_pending_blocks().get_block(true);
        assert!(pending.is_some());
        let pending = pending.unwrap();
        assert_eq!(pending.transactions.len(), 2);

        let overrides = test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .expect("should be set from txn execution");

        assert!(overrides.contains_key(&test.address(User::Alice)));
        assert_eq!(
            overrides
                .get(&test.address(User::Bob))
                .expect("should be set as txn receiver")
                .balance
                .expect("should be changed due to receiving funds"),
            U256::from(100_200_000)
        );

        test.new_canonical_block(vec![test.build_transaction_to_send_eth_with_nonce(
            User::Alice,
            User::Bob,
            100,
            0,
        )])
        .await;

        let pending = test.flashblocks.get_pending_blocks().get_block(true);
        assert!(pending.is_some());
        let pending = pending.unwrap();
        assert_eq!(pending.transactions.len(), 2);

        let overrides = test
            .flashblocks
            .get_pending_blocks()
            .get_state_overrides()
            .expect("should be set from txn execution");

        assert!(overrides.contains_key(&test.address(User::Alice)));
        assert_eq!(
            overrides
                .get(&test.address(User::Bob))
                .expect("should be set as txn receiver")
                .balance
                .expect("should be changed due to receiving funds"),
            U256::from(100_100_100)
        );
    }

    #[tokio::test]
    async fn test_nonce_uses_pending_canon_block_instead_of_latest() {
        // Test for race condition when a canon block comes in but user
        // requests their nonce prior to the StateProcessor processing the canon block
        // causing it to return an n+1 nonce instead of n
        // because underlying reth node `latest` block is already updated, but
        // relevant pending state has not been cleared yet
        init_logging_once();
        let mut test = TestHarness::new();

        test.send_flashblock(FlashblockBuilder::new_base(&test).build())
            .await;
        test.send_flashblock(
            FlashblockBuilder::new(&test, 1)
                .with_transactions(vec![test.build_transaction_to_send_eth(
                    User::Alice,
                    User::Bob,
                    100,
                )])
                .build(),
        )
        .await;

        let pending_nonce = test
            .provider
            .basic_account(&test.address(User::Alice))
            .unwrap()
            .unwrap()
            .nonce
            + test
                .flashblocks
                .get_pending_blocks()
                .get_transaction_count(test.address(User::Alice))
                .to::<u64>();
        assert_eq!(pending_nonce, 1);

        test.new_canonical_block_without_processing(vec![
            test.build_transaction_to_send_eth_with_nonce(User::Alice, User::Bob, 100, 0)
        ])
        .await;

        let pending_nonce = test
            .provider
            .basic_account(&test.address(User::Alice))
            .unwrap()
            .unwrap()
            .nonce
            + test
                .flashblocks
                .get_pending_blocks()
                .get_transaction_count(test.address(User::Alice))
                .to::<u64>();

        // This is 2, because canon block has reached the underlying chain
        // but the StateProcessor hasn't processed it
        // so pending nonce is effectively double-counting the same transaction, leading to a nonce of 2
        assert_eq!(pending_nonce, 2);

        // On the RPC level, we correctly return 1 because we
        // use the pending canon block instead of the latest block when fetching
        // onchain nonce count to compute
        // pending_nonce = onchain_nonce + pending_txn_count
        let canon_block = test
            .flashblocks
            .get_pending_blocks()
            .get_canonical_block_number();
        let canon_state_provider = test
            .provider
            .state_by_block_number_or_tag(canon_block)
            .unwrap();
        let canon_nonce = canon_state_provider
            .account_nonce(&test.address(User::Alice))
            .unwrap()
            .unwrap();
        let pending_nonce = canon_nonce
            + test
                .flashblocks
                .get_pending_blocks()
                .get_transaction_count(test.address(User::Alice))
                .to::<u64>();
        assert_eq!(pending_nonce, 1);
    }

    #[tokio::test]
    async fn test_missing_receipts_will_not_process() {
        init_logging_once();
        let test = TestHarness::new();

        test.send_flashblock(FlashblockBuilder::new_base(&test).build())
            .await;

        let current_block = test.flashblocks.get_pending_blocks().get_block(true);

        test.send_flashblock(
            FlashblockBuilder::new(&test, 1)
                .with_transactions(vec![test.build_transaction_to_send_eth(
                    User::Alice,
                    User::Bob,
                    100,
                )])
                .with_receipts(HashMap::default()) // Clear the receipts
                .build(),
        )
        .await;

        let pending_block = test.flashblocks.get_pending_blocks().get_block(true);

        // When the flashblock is invalid, the chain doesn't progress
        assert_eq!(pending_block.unwrap().hash(), current_block.unwrap().hash());
    }

    #[tokio::test]
    async fn test_flashblock_for_new_canonical_block_clears_older_flashblocks_if_non_zero_index() {
        init_logging_once();
        let test = TestHarness::new();

        test.send_flashblock(FlashblockBuilder::new_base(&test).build())
            .await;

        let current_block = test
            .flashblocks
            .get_pending_blocks()
            .get_block(true)
            .expect("should be a block");

        assert_eq!(current_block.header().number, 1);
        assert_eq!(current_block.transactions.len(), 1);

        test.send_flashblock(
            FlashblockBuilder::new(&test, 1)
                .with_canonical_block_number(100)
                .build(),
        )
        .await;

        let current_block = test.flashblocks.get_pending_blocks().get_block(true);
        assert!(current_block.is_none());
    }

    #[tokio::test]
    async fn test_flashblock_for_new_canonical_block_works_if_sequential() {
        init_logging_once();
        let test = TestHarness::new();

        test.send_flashblock(FlashblockBuilder::new_base(&test).build())
            .await;

        let current_block = test
            .flashblocks
            .get_pending_blocks()
            .get_block(true)
            .expect("should be a block");

        assert_eq!(current_block.header().number, 1);
        assert_eq!(current_block.transactions.len(), 1);

        test.send_flashblock(
            FlashblockBuilder::new_base(&test)
                .with_canonical_block_number(1)
                .build(),
        )
        .await;

        let current_block = test
            .flashblocks
            .get_pending_blocks()
            .get_block(true)
            .expect("should be a block");

        assert_eq!(current_block.header().number, 2);
        assert_eq!(current_block.transactions.len(), 1);
    }

    #[tokio::test]
    async fn test_non_sequential_payload_clears_pending_state() {
        init_logging_once();
        let test = TestHarness::new();

        assert!(test
            .flashblocks
            .get_pending_blocks()
            .get_block(true)
            .is_none());

        test.send_flashblock(FlashblockBuilder::new_base(&test).build())
            .await;

        // Just the block info transaction
        assert_eq!(
            test.flashblocks
                .get_pending_blocks()
                .get_block(true)
                .expect("should be set")
                .transactions
                .len(),
            1
        );

        test.send_flashblock(
            FlashblockBuilder::new(&test, 3)
                .with_transactions(vec![test.build_transaction_to_send_eth(
                    User::Alice,
                    User::Bob,
                    100,
                )])
                .build(),
        )
        .await;

        assert!(test.flashblocks.get_pending_blocks().is_none());
    }

    #[tokio::test]
    async fn test_duplicate_flashblock_ignored() {
        init_logging_once();
        let test = TestHarness::new();

        test.send_flashblock(FlashblockBuilder::new_base(&test).build())
            .await;

        let fb = FlashblockBuilder::new(&test, 1)
            .with_transactions(vec![test.build_transaction_to_send_eth(
                User::Alice,
                User::Bob,
                100_000,
            )])
            .build();

        test.send_flashblock(fb.clone()).await;
        let block = test.flashblocks.get_pending_blocks().get_block(true);

        test.send_flashblock(fb.clone()).await;
        let block_two = test.flashblocks.get_pending_blocks().get_block(true);

        assert_eq!(block, block_two);
    }

    #[tokio::test]
    async fn test_progress_canonical_blocks_without_flashblocks() {
        init_logging_once();
        let mut test = TestHarness::new();

        let genesis_block = test.current_canonical_block();
        assert_eq!(genesis_block.number, 0);
        assert_eq!(genesis_block.transaction_count(), 0);
        assert!(test
            .flashblocks
            .get_pending_blocks()
            .get_block(true)
            .is_none());

        test.new_canonical_block(vec![test.build_transaction_to_send_eth(
            User::Alice,
            User::Bob,
            100,
        )])
        .await;

        let block_one = test.current_canonical_block();
        assert_eq!(block_one.number, 1);
        assert_eq!(block_one.transaction_count(), 2);
        assert!(test
            .flashblocks
            .get_pending_blocks()
            .get_block(true)
            .is_none());

        test.new_canonical_block(vec![
            test.build_transaction_to_send_eth(User::Bob, User::Charlie, 100),
            test.build_transaction_to_send_eth(User::Charlie, User::Alice, 1000),
        ])
        .await;

        let block_two = test.current_canonical_block();
        assert_eq!(block_two.number, 2);
        assert_eq!(block_two.transaction_count(), 3);
        assert!(test
            .flashblocks
            .get_pending_blocks()
            .get_block(true)
            .is_none());
    }
}
