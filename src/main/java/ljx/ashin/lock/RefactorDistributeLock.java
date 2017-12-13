package ljx.ashin.lock;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 分布式锁
 * Created by Ashin Liang on 2017/12/13.
 */
public class RefactorDistributeLock {

    //连接串
    private static final String connectString = "172.17.45.18:2181";

    private static final int sessionTimeOut = 3000;//超时时间,2S

    private static  final String PARENT_LOCK_ROOT = "/parentLock";//父目录

    private static final String SUB_LOCK = "/sub";//子目录

    private ZooKeeper zk = null;

    private String thisNodePath = null;//当前节点路径
    private String waitNodePath = null;//前面的一个节点路径

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 连接zookeeper
     */
    public void connectZk(){
        try {
            zk = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
                public void process(WatchedEvent watchedEvent) {//监听事件
                    if (watchedEvent.getState()== Event.KeeperState.SyncConnected){//连接成功
                        countDownLatch.countDown();
                    }
                    //监听前一个节点的变化
                    if (watchedEvent.getType()== Event.EventType.NodeChildrenChanged
                            &&watchedEvent.getPath().equals(waitNodePath)){
                        List<String> nodeList = getSortNodeList();//节点列表
                        if (nodeList.size()==1){
                            //只有一个节点
                            businessProcess(thisNodePath);
                        }
                    }
                }
            });
        } catch (IOException e) {
            System.out.println("zookeeper连接出错");
            e.printStackTrace();
        }
    }

    /**
     * 获取排序后的节点列表
     * @return
     */
    private List<String> getSortNodeList(){
        List<String> nodeList = new ArrayList<String>();
        try {
            nodeList = zk.getChildren(PARENT_LOCK_ROOT,true);
            Collections.sort(nodeList);
        } catch (KeeperException e) {
            System.out.println("获取节点列表出错："+e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println("获取节点列表出错："+e.getMessage());
            e.printStackTrace();
        }
        return nodeList;
    }

    /**
     * 业务处理
     * @param nodePath
     */
    private void businessProcess(String nodePath){
        try {
            System.out.println(nodePath+"获得了锁，进行业务处理...");
            Thread.sleep(2*1000);
            System.out.println(nodePath+"业务处理结束");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            System.out.println("将删除节点:"+nodePath+"释放锁");
            try {
                zk.delete(PARENT_LOCK_ROOT+"/"+nodePath,-1);//-1表示所有的版本
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建节点
     */
    private void createAndListenNode(){
//        zk.create(PARENT_LOCK_ROOT+SUB_LOCK, ZooDefs.Ids.OPEN_ACL_UNSAFE,)
    }
}
