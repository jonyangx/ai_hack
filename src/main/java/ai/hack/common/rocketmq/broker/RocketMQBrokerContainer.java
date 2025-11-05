package ai.hack.common.rocketmq.broker;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RocketMQ Broker 容器类
 */
public class RocketMQBrokerContainer {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQBrokerContainer.class);

    private final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private final MessageStoreConfig messageStoreConfig;
    private BrokerController brokerController;
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 私有构造函数，通过 Builder 模式创建实例
     *
     * 实现思路：
     * 1. 初始化所有 RocketMQ 配置对象（Broker、网络、消息存储）
     * 2. 从 Builder 中获取配置参数并设置到相应的配置对象
     * 3. 配置网络监听端口
     * 4. 设置消息存储路径并创建目录
     * 5. 配置自动创建 Topic 的选项
     * 6. 记录初始化完成日志
     *
     * @param builder 构建器对象，包含所有配置参数
     */
    private RocketMQBrokerContainer(Builder builder) {
        // 步骤1: 创建所有必要的配置对象
        this.brokerConfig = new BrokerConfig();           // Broker 核心配置
        this.nettyServerConfig = new NettyServerConfig(); // Netty 服务器配置
        this.nettyClientConfig = new NettyClientConfig(); // Netty 客户端配置
        this.messageStoreConfig = new MessageStoreConfig(); // 消息存储配置

        // 步骤2: 设置 Broker 基本信息
        this.brokerConfig.setBrokerName(builder.brokerName);        // Broker 名称
        this.brokerConfig.setBrokerClusterName(builder.clusterName); // 所属集群名称
        this.brokerConfig.setBrokerId(builder.brokerId);            // Broker ID (0=Master, >0=Slave)
        this.brokerConfig.setNamesrvAddr(builder.namesrvAddr);       // NameServer 地址

        // 步骤3: 配置网络监听端口
        this.nettyServerConfig.setListenPort(builder.listenPort);

        // 步骤4: 配置消息存储路径
        String storePathRootDir = builder.storePathRootDir;
        this.messageStoreConfig.setStorePathRootDir(storePathRootDir); // 存储根目录
        this.messageStoreConfig.setStorePathCommitLog(storePathRootDir + File.separator + "commitlog"); // CommitLog 路径

        // 创建存储目录（如果不存在）
        File storeDir = new File(storePathRootDir);
        if (!storeDir.exists()) {
            storeDir.mkdirs(); // 递归创建目录
        }

        // 步骤5: 配置自动创建 Topic 选项
        if (builder.autoCreateTopicEnable != null) {
            this.brokerConfig.setAutoCreateTopicEnable(builder.autoCreateTopicEnable);
        }

        // 步骤6: 记录初始化完成日志
        LOG.info("RocketMQ Broker container initialized - name: {}, cluster: {}, port: {}, namesrv: {}",
                builder.brokerName, builder.clusterName, builder.listenPort, builder.namesrvAddr);
    }

    /**
     * 启动 RocketMQ Broker 服务
     *
     * 实现思路：
     * 1. 使用原子操作检查并设置启动状态，避免重复启动
     * 2. 记录启动开始日志
     * 3. 创建 BrokerController 实例，传入所有配置
     * 4. 初始化 BrokerController（加载配置、启动内部组件）
     * 5. 如果初始化失败，清理资源并抛出异常
     * 6. 启动 Broker 服务，开始接受客户端连接
     * 7. 记录启动成功信息
     *
     * @throws Exception 启动过程中出现异常
     */
    public void start() throws Exception {
        // 步骤1: 原子操作检查启动状态，避免并发问题
        if (started.compareAndSet(false, true)) {
            // 步骤2: 记录启动开始日志
            LOG.info("Starting RocketMQ Broker...");

            // 步骤3: 创建 BrokerController，这是 RocketMQ Broker 的核心控制器
            this.brokerController = new BrokerController(
                    brokerConfig,        // Broker 核心配置
                    nettyServerConfig,   // Netty 服务器配置
                    nettyClientConfig,   // Netty 客户端配置
                    messageStoreConfig   // 消息存储配置
            );

            // 步骤4: 初始化 BrokerController（加载配置、初始化存储、启动内部线程等）
            boolean initResult = brokerController.initialize();
            if (!initResult) {
                // 初始化失败时进行资源清理
                brokerController.shutdown();
                throw new RuntimeException("Failed to initialize RocketMQ Broker");
            }

            // 步骤5: 启动 Broker 服务，开始监听端口并接受客户端连接
            brokerController.start();

            // 步骤6: 记录启动成功信息
            LOG.info("RocketMQ Broker started successfully");
            LOG.info("Broker name: {}, cluster: {}, port: {}",
                    brokerConfig.getBrokerName(),
                    brokerConfig.getBrokerClusterName(),
                    nettyServerConfig.getListenPort());
        } else {
            // Broker 已经启动，记录警告日志
            LOG.warn("RocketMQ Broker is already started");
        }
    }

    /**
     * 关闭 RocketMQ Broker 服务
     *
     * 实现思路：
     * 1. 使用原子操作检查并设置关闭状态，避免重复关闭
     * 2. 记录关闭开始日志
     * 3. 关闭 BrokerController（停止服务、释放资源、清理线程）
     * 4. 记录关闭完成日志
     * 5. 如果 Broker 未启动，记录警告日志
     */
    public void shutdown() {
        // 步骤1: 原子操作检查关闭状态，避免并发问题
        if (started.compareAndSet(true, false)) {
            // 步骤2: 记录关闭开始日志
            LOG.info("Shutting down RocketMQ Broker...");

            // 步骤3: 关闭 BrokerController，停止所有服务和资源
            if (brokerController != null) {
                brokerController.shutdown();
            }

            // 步骤4: 记录关闭完成日志
            LOG.info("RocketMQ Broker stopped");
        } else {
            // Broker 未启动，记录警告日志
            LOG.warn("RocketMQ Broker is not running");
        }
    }

    public boolean isRunning() {
        return started.get();
    }

    public int getListenPort() {
        return nettyServerConfig.getListenPort();
    }

    public String getBrokerName() {
        return brokerConfig.getBrokerName();
    }

    public String getClusterName() {
        return brokerConfig.getBrokerClusterName();
    }

    public long getBrokerId() {
        return brokerConfig.getBrokerId();
    }

    public String getNamesrvAddr() {
        return brokerConfig.getNamesrvAddr();
    }

    public String getBrokerAddress() {
        return "127.0.0.1:" + getListenPort();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String brokerName = "broker-a";
        private String clusterName = "AIHackCluster";
        private long brokerId = 0L;
        private String namesrvAddr = "127.0.0.1:9876";
        private int listenPort = 10911;
        private String storePathRootDir = System.getProperty("user.home") + File.separator + "rocketmq-broker-data";
        private Boolean autoCreateTopicEnable = true;

        public Builder brokerName(String brokerName) {
            this.brokerName = brokerName;
            return this;
        }

        public Builder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public Builder brokerId(long brokerId) {
            this.brokerId = brokerId;
            return this;
        }

        public Builder namesrvAddr(String namesrvAddr) {
            this.namesrvAddr = namesrvAddr;
            return this;
        }

        public Builder listenPort(int listenPort) {
            this.listenPort = listenPort;
            return this;
        }

        public Builder storePathRootDir(String storePathRootDir) {
            this.storePathRootDir = storePathRootDir;
            return this;
        }

        public Builder autoCreateTopicEnable(boolean autoCreateTopicEnable) {
            this.autoCreateTopicEnable = autoCreateTopicEnable;
            return this;
        }

        public RocketMQBrokerContainer build() {
            return new RocketMQBrokerContainer(this);
        }
    }
}
