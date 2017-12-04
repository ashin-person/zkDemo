package ljx.ashin.distribute;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * zookeeper分布式服务端
 * Created by AshinLiang on 2017/11/23.
 */
public class ZKDistributeServer {

    private static final String connectString = "172.26.15.11:2181";//连接串
    private static final int sessionTimeout = 30000;//超时设置

    private String parentNode = "/servers";//父节点

    private ZooKeeper zk = null;

    /**
     * 连接上ZK
     */
    private void connectZK(){
        try {
            zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    //处理函数
                    System.out.println("事件类型："+watchedEvent.getType()+" 路径"+watchedEvent.getPath());
                    try {
                        zk.getChildren("/",true);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

           /* String path = zk.create(parentNode,"parentNodeValue".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            System.out.println(path);*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 向服务器注册节点
     * @param node
     * @return
     */
    private String registerServer(String node){
        String nodePath = null;
        try {
            //判断父节点是否存在，不存在则创建
           if (null==zk.exists(parentNode,false)){
               zk.create(parentNode,"parentNodeValue".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
           }
           nodePath = zk.create(parentNode+"/"+node,node.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("注册的节点路径为："+nodePath);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return nodePath;
    }

    /**
     * 业务处理
     * @param node
     */
    public void proccessBussiness(String node){
        System.out.println("服务器:"+node+"已经启动。。。");
        System.out.println("进行业务处理");
        try {
            Thread.sleep(Long.MAX_VALUE);//睡眠，防止线程结束
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String param = args[0];//输入的第一个参数
        ZKDistributeServer zkDistributeServer = new ZKDistributeServer();
        zkDistributeServer.connectZK();
        zkDistributeServer.registerServer(param);
        zkDistributeServer.proccessBussiness(param);
    }
}
