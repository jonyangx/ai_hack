package ai.hack.common.rocketmq.namesrv;

import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RocketMQ NameServer 容器类
 */
public class RocketMQNameServerContainer {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQNameServerContainer.class);

    private final NamesrvConfig namesrvConfig;
    private final NettyServerConfig nettyServerConfig;
    private NamesrvController namesrvController;
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 私有构造函数，通过 Builder 模式创建实例
     *
     * 实现思路：
     * 1. 初始化 RocketMQ NameServer 配置对象
     * 2. 配置网络监听端口
     * 3. 设置 RocketMQ 主目录并创建目录（如果不存在）
     * 4. 配置 KV 配置文件的路径
     * 5. 记录初始化完成日志
     *
     * @param builder 构建器对象，包含所有配置参数
     */
    private RocketMQNameServerContainer(Builder builder) {
        // 步骤1: 初始化 NameServer 配置对象
        this.namesrvConfig = new NamesrvConfig();
        this.nettyServerConfig = new NettyServerConfig();

        // 步骤2: 配置网络监听端口
        this.nettyServerConfig.setListenPort(builder.listenPort);

        // 步骤3: 配置 RocketMQ 主目录（用于存储配置和数据）
        if (builder.rocketmqHome != null) {
            this.namesrvConfig.setRocketmqHome(builder.rocketmqHome);
            File homeDir = new File(builder.rocketmqHome);
            if (!homeDir.exists()) {
                homeDir.mkdirs(); // 递归创建目录
            }
        }

        // 步骤4: 配置 KV 配置文件路径（用于存储键值对配置）
        if (builder.kvConfigPath != null) {
            this.namesrvConfig.setKvConfigPath(builder.kvConfigPath);
        }

        // 步骤5: 记录初始化完成日志
        LOG.info("RocketMQ NameServer container initialized with port: {}, home: {}",
                builder.listenPort, builder.rocketmqHome);
    }

    /**
     * 启动 RocketMQ NameServer 服务
     *
     * 实现思路：
     * 1. 使用原子操作检查并设置启动状态，避免重复启动
     * 2. 记录启动开始日志
     * 3. 创建 NamesrvController 实例，传入配置参数
     * 4. 初始化 NamesrvController（加载配置、启动内部组件）
     * 5. 如果初始化失败，清理资源并抛出异常
     * 6. 启动 NameServer 服务，开始监听端口
     * 7. 记录启动成功信息和地址
     *
     * @throws Exception 启动过程中出现异常
     */
    public void start() throws Exception {
        // 步骤1: 原子操作检查启动状态，避免并发问题
        if (started.compareAndSet(false, true)) {
            // 步骤2: 记录启动开始日志
            LOG.info("Starting RocketMQ NameServer...");

            // 步骤3: 创建 NamesrvController，这是 RocketMQ NameServer 的核心控制器
            this.namesrvController = new NamesrvController(namesrvConfig, nettyServerConfig);

            // 步骤4: 初始化 NamesrvController（加载配置、初始化路由表、启动内部线程等）
            boolean initResult = namesrvController.initialize();
            if (!initResult) {
                // 初始化失败时进行资源清理
                namesrvController.shutdown();
                throw new RuntimeException("Failed to initialize RocketMQ NameServer");
            }

            // 步骤5: 启动 NameServer 服务，开始监听端口并接受 Broker 注册
            namesrvController.start();

            // 步骤6: 记录启动成功信息
            LOG.info("RocketMQ NameServer started successfully on port: {}",
                    nettyServerConfig.getListenPort());
            LOG.info("NameServer address: {}:{}",
                    getAddress(), nettyServerConfig.getListenPort());
        } else {
            // NameServer 已经启动，记录警告日志
            LOG.warn("RocketMQ NameServer is already started");
        }
    }

    /**
     * 关闭 RocketMQ NameServer 服务
     *
     * 实现思路：
     * 1. 使用原子操作检查并设置关闭状态，避免重复关闭
     * 2. 记录关闭开始日志
     * 3. 关闭 NamesrvController（停止服务、释放资源、清理线程）
     * 4. 记录关闭完成日志
     * 5. 如果 NameServer 未启动，记录警告日志
     */
    public void shutdown() {
        // 步骤1: 原子操作检查关闭状态，避免并发问题
        if (started.compareAndSet(true, false)) {
            // 步骤2: 记录关闭开始日志
            LOG.info("Shutting down RocketMQ NameServer...");

            // 步骤3: 关闭 NamesrvController，停止所有服务和资源
            if (namesrvController != null) {
                namesrvController.shutdown();
            }

            // 步骤4: 记录关闭完成日志
            LOG.info("RocketMQ NameServer stopped");
        } else {
            // NameServer 未启动，记录警告日志
            LOG.warn("RocketMQ NameServer is not running");
        }
    }

    public boolean isRunning() {
        return started.get();
    }

    public int getListenPort() {
        return nettyServerConfig.getListenPort();
    }

    public String getAddress() {
        return "127.0.0.1";
    }

    public String getFullAddress() {
        return getAddress() + ":" + getListenPort();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int listenPort = 9876;
        private String rocketmqHome = System.getProperty("user.home") + File.separator + "rocketmq-data";
        private String kvConfigPath = null;

        public Builder listenPort(int listenPort) {
            this.listenPort = listenPort;
            return this;
        }

        public Builder rocketmqHome(String rocketmqHome) {
            this.rocketmqHome = rocketmqHome;
            return this;
        }

        public Builder kvConfigPath(String kvConfigPath) {
            this.kvConfigPath = kvConfigPath;
            return this;
        }

        public RocketMQNameServerContainer build() {
            return new RocketMQNameServerContainer(this);
        }
    }
}
