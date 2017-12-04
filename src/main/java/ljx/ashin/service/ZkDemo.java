package ljx.ashin.service;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by AshinLiang on 2017/11/18.
 */

public class ZkDemo {

    private ZooKeeper zk = null;

    /**
     * 初始化zookeeper客户端
     */
    @Before
    public void init(){
        String connectString = "172.26.15.11:2181";//连接窜
        int sessionTimeout = 30000;
        try {
            System.out.println("==开始初始化zookeeper==");
            zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    System.out.println("触发了事件类型:"+watchedEvent.getType()+"事件的路径:"+watchedEvent.getPath());
                    try {
                        zk.getChildren("/",true);
                    } catch  (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建节点
     */
    @Test
    public void testCreateZkNode(){
        if(zk!=null){
            try {
                String data = "zkSum";
                String path = zk.create("/ashinA",data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                System.out.println("在"+path+"中添加了数据:"+data);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else {
            System.out.println("zookeeper为null");
        }
    }

    /**
     * 获取子节点
     */
    @Test
    public void testGetChildren(){
        try {
            System.out.println("调用方法获取子节点");
            List<String> childrens = zk.getChildren("/",true);
            for (String child:childrens) {
                System.out.println("子节点："+child);
            }
            Thread.sleep(Integer.MAX_VALUE);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除节点
     */
    @Test
    public void testDeleteNode(){
        try {
            zk.delete("/ashinZK0000000002",-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * 检查节点是否存在
     */
    @Test
    public void testExistNode(){
        try {
            Stat stat = zk.exists("/ashinB",false);
            System.out.println(stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
