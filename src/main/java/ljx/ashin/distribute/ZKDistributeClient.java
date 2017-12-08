package ljx.ashin.distribute;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * zookeeper分布式的客户端
 * Created by Ashin Liang on 2017/12/8.
 */
public class ZKDistributeClient {
    //连接串
    private static final String connectString = "172.26.15.11:2181";
    //超时时间
    private static final int sessionTimeOut = 3000;
    //服务器列表
    private volatile List<String> serverList;

    private static final String path = "/servers";

    private ZooKeeper zooKeeper = null;

    //1、连接zookeeper
    public void connectZK(){
        try {
            zooKeeper = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getType()== Event.EventType.NodeChildrenChanged){
                        System.out.println("子目录节点发生了变化");
                        getServers(path);

                    }

                    System.out.println("监控到数据发生了变化:"+watchedEvent.getPath()+" "+watchedEvent.getType());
                    getServers(path);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取服务器列表
     * @return
     */
    private List<String> getServers(String path){
        try {
            List<String> list = this.zooKeeper.getChildren(path,true);
            serverList = list;
            for (String ip :serverList){
                System.out.println("服务器IP："+ip);
            }
            this.zooKeeper.getChildren(path,true);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void bussinessProcess(){
        System.out.println("做业务处理");
        System.out.println("获取到的服务器列表");
        /*for (String server:serverList){
            System.out.println("服务器:"+server);
        }*/
    }

    public static void main(String[] args) {
        ZKDistributeClient client = new ZKDistributeClient();
        client.connectZK();
        client.getServers(path);
        client.bussinessProcess();
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
