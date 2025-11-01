use alloy_consensus::Receipt;
use alloy_genesis::Genesis;
use alloy_primitives::map::HashMap;
use alloy_primitives::{address, b256, bytes, Address, Bytes, LogData, TxHash, B256, U256};
use alloy_provider::RootProvider;
use alloy_rpc_client::RpcClient;
use alloy_rpc_types_engine::PayloadId;
use base_reth_flashblocks_rpc::rpc::{EthApiExt, EthApiOverrideServer};
use base_reth_flashblocks_rpc::state::FlashblocksState;
use base_reth_flashblocks_rpc::subscription::{Flashblock, FlashblocksReceiver, Metadata};
use op_alloy_consensus::OpDepositReceipt;
use op_alloy_network::Optimism;
use reth::args::{DiscoveryArgs, NetworkArgs, RpcServerArgs};
use reth::builder::{Node, NodeBuilder, NodeConfig, NodeHandle};
use reth::chainspec::Chain;
use reth::core::exit::NodeExitFuture;
use reth::tasks::TaskManager;
use reth_optimism_chainspec::OpChainSpecBuilder;
use reth_optimism_node::args::RollupArgs;
use reth_optimism_node::OpNode;
use reth_optimism_primitives::OpReceipt;
use reth_provider::providers::BlockchainProvider;
use reth_rpc_eth_api::RpcReceipt;
use rollup_boost::{ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1};
use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Once;
use tokio::sync::{mpsc, oneshot};

pub const BLOCK_INFO_TXN: Bytes = bytes!("0x7ef90104a06c0c775b6b492bab9d7e81abdf27f77cafb698551226455a82f559e0f93fea3794deaddeaddeaddeaddeaddeaddeaddeaddead00019442000000000000000000000000000000000000158080830f424080b8b0098999be000008dd00101c1200000000000000020000000068869d6300000000015f277f000000000000000000000000000000000000000000000000000000000d42ac290000000000000000000000000000000000000000000000000000000000000001abf52777e63959936b1bf633a2a643f0da38d63deffe49452fed1bf8a44975d50000000000000000000000005050f69a9786f081509234f1a7f4684b5e5b76c9000000000000000000000000");

pub const BLOCK_INFO_TXN_HASH: B256 =
    b256!("0xba56c8b0deb460ff070f8fca8e2ee01e51a3db27841cc862fdd94cc1a47662b6");

pub const TEST_ADDRESS: Address = address!("0x1234567890123456789012345678901234567890");
pub const PENDING_BALANCE: u64 = 4660;

pub const DEPOSIT_SENDER: Address = address!("0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001");
pub const TX_SENDER: Address = address!("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266");

pub const DEPOSIT_TX_HASH: TxHash =
    b256!("0x2be2e6f8b01b03b87ae9f0ebca8bbd420f174bef0fbcc18c7802c5378b78f548");
pub const TRANSFER_ETH_HASH: TxHash =
    b256!("0xbb079fbde7d12fd01664483cd810e91014113e405247479e5615974ebca93e4a");

pub const DEPLOYMENT_HASH: TxHash =
    b256!("0x2b14d58c13406f25a78cfb802fb711c0d2c27bf9eccaec2d1847dc4392918f63");

pub const INCREMENT_HASH: TxHash =
    b256!("0x993ad6a332752f6748636ce899b3791e4a33f7eece82c0db4556c7339c1b2929");
pub const INCREMENT2_HASH: TxHash =
    b256!("0x617a3673399647d12bb82ec8eba2ca3fc468e99894bcf1c67eb50ef38ee615cb");

pub const COUNTER_ADDRESS: Address = address!("0xe7f1725e7734ce288f8367e1bb143e90bb3f0512");

// Test log topics - these represent common events
pub const TEST_LOG_TOPIC_0: B256 =
    b256!("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"); // Transfer event
pub const TEST_LOG_TOPIC_1: B256 =
    b256!("0x000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266"); // From address
pub const TEST_LOG_TOPIC_2: B256 =
    b256!("0x0000000000000000000000001234567890123456789012345678901234567890"); // To address

pub const DEPOSIT_TX: Bytes = bytes!("0x7ef8f8a042a8ae5ec231af3d0f90f68543ec8bca1da4f7edd712d5b51b490688355a6db794deaddeaddeaddeaddeaddeaddeaddeaddead00019442000000000000000000000000000000000000158080830f424080b8a4440a5e200000044d000a118b00000000000000040000000067cb7cb0000000000077dbd4000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000014edd27304108914dd6503b19b9eeb9956982ef197febbeeed8a9eac3dbaaabdf000000000000000000000000fc56e7272eebbba5bc6c544e159483c4a38f8ba3");
pub const TRANSFER_ETH_TX: Bytes = bytes!("0x02f87383014a3480808449504f80830186a094deaddeaddeaddeaddeaddeaddeaddeaddead00018ad3c21bcb3f6efc39800080c0019f5a6fe2065583f4f3730e82e5725f651cbbaf11dc1f82c8d29ba1f3f99e5383a061e0bf5dfff4a9bc521ad426eee593d3653c5c330ae8a65fad3175d30f291d31");
pub const DEPLOYMENT_TX: Bytes = bytes!("0x02f9029483014a3401808449504f80830493e08080b9023c608060405260015f55600180553480156016575f80fd5b50610218806100245f395ff3fe608060405234801561000f575f80fd5b5060043610610060575f3560e01c80631d63e24d146100645780637477f70014610082578063a87d942c146100a0578063ab57b128146100be578063d09de08a146100c8578063d631c639146100d2575b5f80fd5b61006c6100f0565b6040516100799190610155565b60405180910390f35b61008a6100f6565b6040516100979190610155565b60405180910390f35b6100a86100fb565b6040516100b59190610155565b60405180910390f35b6100c6610103565b005b6100d061011c565b005b6100da610134565b6040516100e79190610155565b60405180910390f35b60015481565b5f5481565b5f8054905090565b60015f8154809291906101159061019b565b9190505550565b5f8081548092919061012d9061019b565b9190505550565b5f600154905090565b5f819050919050565b61014f8161013d565b82525050565b5f6020820190506101685f830184610146565b92915050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52601160045260245ffd5b5f6101a58261013d565b91507fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff82036101d7576101d661016e565b5b60018201905091905056fea264697066735822122025c7e02ddf460dece9c1e52a3f9ff042055b58005168e7825d7f6c426288c27164736f6c63430008190033c001a02f196658032e0b003bcd234349d63081f5d6c2785264c6fec6b25ad877ae326aa0290c9f96f4501439b07a7b5e8e938f15fc30a9c15db3fc5e654d44e1f522060c");
pub const INCREMENT_TX: Bytes = bytes!("0x02f86d83014a3402808449504f8082abe094e7f1725e7734ce288f8367e1bb143e90bb3f05128084d09de08ac080a0a9c1a565668084d4052bbd9bc3abce8555a06aed6651c82c2756ac8a83a79fa2a03427f440ce4910a5227ea0cedb60b06cf0bea2dbbac93bd37efa91a474c29d89");
pub const INCREMENT2_TX: Bytes = bytes!("0x02f86d83014a3403808449504f8082abe094e7f1725e7734ce288f8367e1bb143e90bb3f05128084ab57b128c001a03a155b8c81165fc8193aa739522c2a9e432e274adea7f0b90ef2b5078737f153a0288d7fad4a3b0d1e7eaf7fab63b298393a5020bf11d91ff8df13b235410799e2");

pub struct NodeContext {
    sender: mpsc::Sender<(Flashblock, oneshot::Sender<()>)>,
    http_api_addr: SocketAddr,
    _node_exit_future: NodeExitFuture,
    _node: Box<dyn Any + Sync + Send>,
    _task_manager: TaskManager,
}

impl NodeContext {
    pub async fn send_payload(&self, payload: Flashblock) -> eyre::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender.send((payload, tx)).await?;
        rx.await?;
        Ok(())
    }

    pub async fn provider(&self) -> eyre::Result<RootProvider<Optimism>> {
        let url = format!("http://{}", self.http_api_addr);
        let client = RpcClient::builder().http(url.parse()?);

        Ok(RootProvider::<Optimism>::new(client))
    }

    pub async fn send_test_payloads(&self) -> eyre::Result<()> {
        let base_payload = create_first_payload();
        self.send_payload(base_payload).await?;

        let second_payload = create_second_payload();
        self.send_payload(second_payload).await?;

        Ok(())
    }

    pub async fn send_raw_transaction_sync(
        &self,
        tx: Bytes,
        timeout_ms: Option<u64>,
    ) -> eyre::Result<RpcReceipt<Optimism>> {
        let url = format!("http://{}", self.http_api_addr);
        let client = RpcClient::new_http(url.parse()?);

        let receipt = client
            .request::<_, RpcReceipt<Optimism>>("eth_sendRawTransactionSync", (tx, timeout_ms))
            .await?;

        Ok(receipt)
    }
}

pub async fn setup_node() -> eyre::Result<NodeContext> {
    init_logging_once();
    let tasks = TaskManager::current();
    let exec = tasks.executor();
    const BASE_SEPOLIA_CHAIN_ID: u64 = 84532;

    let genesis: Genesis = serde_json::from_str(include_str!("../assets/genesis.json")).unwrap();
    let chain_spec = Arc::new(
        OpChainSpecBuilder::base_mainnet()
            .genesis(genesis)
            .ecotone_activated()
            .chain(Chain::from(BASE_SEPOLIA_CHAIN_ID))
            .build(),
    );

    let network_config = NetworkArgs {
        discovery: DiscoveryArgs {
            disable_discovery: true,
            ..DiscoveryArgs::default()
        },
        ..NetworkArgs::default()
    };

    // Use with_unused_ports() to let Reth allocate random ports and avoid port collisions
    let node_config = NodeConfig::new(chain_spec.clone())
        .with_network(network_config.clone())
        .with_rpc(RpcServerArgs::default().with_unused_ports().with_http())
        .with_unused_ports();

    let node = OpNode::new(RollupArgs::default());

    // Start websocket server to simulate the builder and send payloads back to the node
    let (sender, mut receiver) = mpsc::channel::<(Flashblock, oneshot::Sender<()>)>(100);

    let NodeHandle {
        node,
        node_exit_future,
    } = NodeBuilder::new(node_config.clone())
        .testing_node(exec.clone())
        .with_types_and_provider::<OpNode, BlockchainProvider<_>>()
        .with_components(node.components_builder())
        .with_add_ons(node.add_ons())
        .extend_rpc_modules(move |ctx| {
            // We are not going to use the websocket connection to send payloads so we use
            // a dummy url.
            let flashblocks_state = Arc::new(FlashblocksState::new(ctx.provider().clone()));
            flashblocks_state.start();

            let api_ext = EthApiExt::new(
                ctx.registry.eth_api().clone(),
                ctx.registry.eth_handlers().filter.clone(),
                flashblocks_state.clone(),
            );

            ctx.modules.replace_configured(api_ext.into_rpc())?;

            tokio::spawn(async move {
                while let Some((payload, tx)) = receiver.recv().await {
                    flashblocks_state.on_flashblock_received(payload);
                    tx.send(()).unwrap();
                }
            });

            Ok(())
        })
        .launch()
        .await?;

    let http_api_addr = node
        .rpc_server_handle()
        .http_local_addr()
        .ok_or_else(|| eyre::eyre!("Failed to get http api address"))?;

    Ok(NodeContext {
        sender,
        http_api_addr,
        _node_exit_future: node_exit_future,
        _node: Box::new(node),
        _task_manager: tasks,
    })
}

pub fn create_first_payload() -> Flashblock {
    Flashblock {
        payload_id: PayloadId::new([0; 8]),
        index: 0,
        base: Some(ExecutionPayloadBaseV1 {
            parent_beacon_block_root: B256::default(),
            parent_hash: B256::default(),
            fee_recipient: Address::ZERO,
            prev_randao: B256::default(),
            block_number: 1,
            gas_limit: 30_000_000,
            timestamp: 0,
            extra_data: Bytes::new(),
            base_fee_per_gas: U256::ZERO,
        }),
        diff: ExecutionPayloadFlashblockDeltaV1 {
            transactions: vec![BLOCK_INFO_TXN],
            ..Default::default()
        },
        metadata: Metadata {
            block_number: 1,
            receipts: {
                let mut receipts = HashMap::default();
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
            new_account_balances: HashMap::default(),
        },
    }
}

pub fn create_second_payload() -> Flashblock {
    Flashblock {
        payload_id: PayloadId::new([0; 8]),
        index: 1,
        base: None,
        diff: ExecutionPayloadFlashblockDeltaV1 {
            state_root: B256::default(),
            receipts_root: B256::default(),
            gas_used: 0,
            block_hash: B256::default(),
            transactions: vec![
                DEPOSIT_TX,
                TRANSFER_ETH_TX,
                DEPLOYMENT_TX,
                INCREMENT_TX,
                INCREMENT2_TX,
            ],
            withdrawals: Vec::new(),
            logs_bloom: Default::default(),
            withdrawals_root: Default::default(),
        },
        metadata: Metadata {
            block_number: 1,
            receipts: {
                let mut receipts = HashMap::default();
                receipts.insert(
                    DEPOSIT_TX_HASH,
                    OpReceipt::Deposit(OpDepositReceipt {
                        inner: Receipt {
                            status: true.into(),
                            cumulative_gas_used: 31000,
                            logs: vec![],
                        },
                        deposit_nonce: Some(4012992u64),
                        deposit_receipt_version: None,
                    }),
                );
                receipts.insert(
                    TRANSFER_ETH_HASH,
                    OpReceipt::Legacy(Receipt {
                        status: true.into(),
                        cumulative_gas_used: 55000,
                        logs: vec![],
                    }),
                );
                receipts.insert(
                    DEPLOYMENT_HASH,
                    OpReceipt::Legacy(Receipt {
                        status: true.into(),
                        cumulative_gas_used: 272279,
                        logs: vec![],
                    }),
                );
                receipts.insert(
                    INCREMENT_HASH,
                    OpReceipt::Legacy(Receipt {
                        status: true.into(),
                        cumulative_gas_used: 272279 + 44000,
                        logs: create_test_logs(),
                    }),
                );
                receipts.insert(
                    INCREMENT2_HASH,
                    OpReceipt::Legacy(Receipt {
                        status: true.into(),
                        cumulative_gas_used: 272279 + 44000 + 44000,
                        logs: vec![],
                    }),
                );
                receipts
            },
            new_account_balances: {
                let mut map = HashMap::default();
                map.insert(TEST_ADDRESS, U256::from(PENDING_BALANCE));
                map.insert(COUNTER_ADDRESS, U256::from(0));
                map
            },
        },
    }
}

pub fn create_test_logs() -> Vec<alloy_primitives::Log> {
    vec![
        alloy_primitives::Log {
            address: COUNTER_ADDRESS,
            data: LogData::new(
                vec![TEST_LOG_TOPIC_0, TEST_LOG_TOPIC_1, TEST_LOG_TOPIC_2],
                bytes!("0x0000000000000000000000000000000000000000000000000de0b6b3a7640000"), // 1 ETH in wei
            )
            .unwrap(),
        },
        alloy_primitives::Log {
            address: TEST_ADDRESS,
            data: LogData::new(
                vec![TEST_LOG_TOPIC_0],
                bytes!("0x0000000000000000000000000000000000000000000000000000000000000001"), // Value: 1
            )
            .unwrap(),
        },
    ]
}
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

pub mod utils;
