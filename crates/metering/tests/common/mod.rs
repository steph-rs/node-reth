#![allow(dead_code)]

use alloy_genesis::Genesis;
use alloy_primitives::Bytes;
use alloy_rpc_client::RpcClient;
use reth::args::{DiscoveryArgs, NetworkArgs, RpcServerArgs};
use reth::builder::{Node, NodeBuilder, NodeConfig, NodeHandle};
use reth::chainspec::Chain;
use reth::core::exit::NodeExitFuture;
use reth::tasks::TaskManager;
use reth_db::{
    init_db,
    mdbx::{DatabaseArguments, MaxReadTransactionDuration, KILOBYTE, MEGABYTE},
    test_utils::{create_test_static_files_dir, tempdir_path, TempDatabase, ERROR_DB_CREATION},
    ClientVersion, DatabaseEnv,
};
use reth_provider::{providers::StaticFileProvider, ProviderFactory};
use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Once;

use base_reth_metering::{MeteringApiImpl, MeteringApiServer};
use reth_optimism_chainspec::OpChainSpecBuilder;
use reth_optimism_node::args::RollupArgs;
use reth_optimism_node::OpNode;
use reth_provider::providers::BlockchainProvider;

static INIT_LOGGING: Once = Once::new();

fn init_logging_once() {
    INIT_LOGGING.call_once(|| {
        if std::env::var("RUST_LOG").is_err() {
            // Silence noisy error logs in tests from task manager, leave others at warn.
            std::env::set_var("RUST_LOG", "warn,reth_tasks=off");
        }
        reth_tracing::init_test_tracing();
    });
}

pub struct NodeContext {
    http_api_addr: SocketAddr,
    _node_exit_future: NodeExitFuture,
    _node: Box<dyn Any + Sync + Send>,
}

impl NodeContext {
    pub async fn rpc_client(&self) -> eyre::Result<RpcClient> {
        let url = format!("http://{}", self.http_api_addr);
        let client = RpcClient::new_http(url.parse()?);
        Ok(client)
    }
}

pub fn create_bundle(
    txs: Vec<Bytes>,
    block_number: u64,
    min_timestamp: Option<u64>,
) -> tips_core::types::Bundle {
    tips_core::types::Bundle {
        txs,
        block_number,
        flashblock_number_min: None,
        flashblock_number_max: None,
        min_timestamp,
        max_timestamp: None,
        reverting_tx_hashes: vec![],
        replacement_uuid: None,
        dropping_tx_hashes: vec![],
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

    let node_config = NodeConfig::new(chain_spec.clone())
        .with_network(network_config.clone())
        .with_rpc(RpcServerArgs::default().with_unused_ports().with_http())
        .with_unused_ports();

    let node = OpNode::new(RollupArgs::default());

    let NodeHandle {
        node,
        node_exit_future,
    } = NodeBuilder::new(node_config.clone())
        .testing_node(exec.clone())
        .with_types_and_provider::<OpNode, BlockchainProvider<_>>()
        .with_components(node.components_builder())
        .with_add_ons(node.add_ons())
        .extend_rpc_modules(move |ctx| {
            let metering_api = MeteringApiImpl::new(ctx.provider().clone());
            ctx.modules.merge_configured(metering_api.into_rpc())?;
            Ok(())
        })
        .launch()
        .await?;

    let http_api_addr = node
        .rpc_server_handle()
        .http_local_addr()
        .ok_or_else(|| eyre::eyre!("Failed to get http api address"))?;

    Ok(NodeContext {
        http_api_addr,
        _node_exit_future: node_exit_future,
        _node: Box::new(node),
    })
}

pub fn create_provider_factory<N: reth::api::NodeTypes>(
    chain_spec: Arc<N::ChainSpec>,
) -> ProviderFactory<reth::api::NodeTypesWithDBAdapter<N, Arc<TempDatabase<DatabaseEnv>>>> {
    let (static_dir, _) = create_test_static_files_dir();
    let db = create_test_db();
    ProviderFactory::new(
        db,
        chain_spec,
        StaticFileProvider::read_write(static_dir.keep()).expect("static file provider"),
    )
}

fn create_test_db() -> Arc<TempDatabase<DatabaseEnv>> {
    let path = tempdir_path();
    let emsg = format!("{ERROR_DB_CREATION}: {path:?}");

    let db = init_db(
        &path,
        DatabaseArguments::new(ClientVersion::default())
            .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded))
            .with_geometry_max_size(Some(4 * MEGABYTE))
            .with_growth_step(Some(4 * KILOBYTE)),
    )
    .expect(&emsg);

    Arc::new(TempDatabase::new(db, path))
}
