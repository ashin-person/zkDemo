package ljx.ashin.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * 简单的分布式锁
 * Created by Ashin Liang on 2017/12/11.
 */
public class SimpleDistributeLock {
    //连接串
    private static final String connectString = "172.17.45.18:2181";

    private static final int sessionTimeOut = 3000;//超时时间,2S

    private static  final String PARENT_LOCK_ROOT = "/parentLock";//父目录

    private static final String SUB_LOCK = "/sub";//子目录

    private ZooKeeper zk = null;

    private String thisPath = null;//当前节点路径
    private String waitPath = null;//前面的一个节点路径

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 连接zk服务器
     */
    public void connectZk(){
        try {
            zk = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
//                    if (zk.getState()== ZooKeeper.States.CONNECTING);
                    if (watchedEvent.getState()== Event.KeeperState.SyncConnected){
                        countDownLatch.countDown();
                    }
                    //如果发生了waitPath的删除事件
                    if (watchedEvent.getType()==Event.EventType.NodeDeleted
                            &&watchedEvent.getPath().equals(waitPath)){
                        //检查是否是最小节点，否则循环监控
                        try {
                            List<String> nodeList = zk.getChildren(PARENT_LOCK_ROOT,false);
                            Collections.sort(nodeList);
                            System.out.println("服务器节点:"+nodeList.toString());
//                            thisPath = thisPath.substring((PARENT_LOCK_ROOT+"/").length());
                            System.out.println("检查当前节点是否是最小节点:"+thisPath);
                            System.out.println("当前节点:"+thisPath);
                            if (thisPath.contains("parentLock")){
                                thisPath = thisPath.substring((PARENT_LOCK_ROOT+"/").length());
                            }
                            int index = nodeList.indexOf(thisPath);
                            if (index==-1){
                                System.out.println("出错了");
                            }else if (index==0){
                                binssenisProcess(thisPath);
                                //创建临时节点
                                createAndListen();
                            }else {
                                //获取前面一个节点，并监听该节点
                                waitPath = PARENT_LOCK_ROOT+SUB_LOCK+nodeList.get(index-1);
                                zk.getData(waitPath,true,new Stat());
                                System.out.println("监听节点:"+waitPath);
                            }
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });

            countDownLatch.await();
            System.out.println("zookeeper连接成功");
            // wait一小会, 让结果更清晰一些
            Thread.sleep(new Random().nextInt(1000));

            if (null!=zk){
                Stat stat = zk.exists(PARENT_LOCK_ROOT,false);
                if (null==stat){//锁根目录不存在则创建
                    String parentPath = zk.create(PARENT_LOCK_ROOT,"parentLockRoot".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                //创建临时节点
               thisPath = zk.create(PARENT_LOCK_ROOT+SUB_LOCK,"subNode".getBytes(),
                       ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
                System.out.println("成功注册了子节点:"+thisPath);
                //获取子节点
               List<String> subNodes = zk.getChildren(PARENT_LOCK_ROOT,false);
                Collections.sort(subNodes);
                System.out.println("服务器节点:"+subNodes.toString());
                if (subNodes.size()==1){
                    System.out.println("只有一个节点");
                    binssenisProcess(thisPath);
                    createAndListen();
                }else {
                    //检查当前节点是否是最小节点
                    thisPath = thisPath.substring((PARENT_LOCK_ROOT+"/").length());
                    int index = subNodes.indexOf(thisPath);
                    if (index==-1){
                        System.out.println("出错了！");
                    }else if (index==0){//最小节点
                        binssenisProcess(thisPath);
                        createAndListen();
                    }else {
                        //获取前面一个节点，并监听该节点
                        waitPath = PARENT_LOCK_ROOT+"/"+subNodes.get(index-1);
                        System.out.println("前面的一个节点:"+waitPath+"  系统时间:"+System.currentTimeMillis());
                        zk.getData(waitPath,true,new Stat());
                        System.out.println("监听节点:"+waitPath);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 业务逻辑处理
     */
    private void binssenisProcess(String path){
        System.out.println("节点"+path+":获取到了锁，进行业务处理");
        try {
            Thread.sleep(5*1000);
            System.out.println("业务处理结束");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            //释放锁，即删除节点
            try {
                if (path.contains("parentLock")){
                    zk.delete(path,-1);
                }else {
                    zk.delete(PARENT_LOCK_ROOT+"/"+path,-1);
                }
                System.out.println("释放掉了锁:"+PARENT_LOCK_ROOT+"/"+path);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建临时节点并监听
     */
    private void createAndListen(){
        //创建临时节点
        try {
            thisPath = zk.create(PARENT_LOCK_ROOT+SUB_LOCK,"subNode".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("成功创建了子节点:"+thisPath);
            isMinNode(thisPath);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断当前节点是否是最小节点
     * @param thisPath
     * @return
     */
    private void isMinNode(String thisPath) throws KeeperException, InterruptedException {
        if (null!=thisPath&&!"".equals(thisPath)) {//获取子节点
            //获取子节点
            List<String> subNodes = zk.getChildren(PARENT_LOCK_ROOT,false);
            Collections.sort(subNodes);
            System.out.println("服务器节点:"+subNodes.toString());
            if (subNodes.size()==1){
                System.out.println("只有一个节点");
                binssenisProcess(thisPath);
                createAndListen();
            }else {
                //检查当前节点是否是最小节点
                thisPath = thisPath.substring((PARENT_LOCK_ROOT+"/").length());
                System.out.println("检查当前节点是否是最小节点:"+thisPath);
                int index = subNodes.indexOf(thisPath);
                if (index==-1){
                    System.out.println("出错了！");

                }else if (index==0){//最小节点
                    binssenisProcess(thisPath);
                    createAndListen();
                }else {
                    //获取前面一个节点，并监听该节点
                    waitPath = PARENT_LOCK_ROOT+"/"+subNodes.get(index-1);
                    System.out.println("前面的一个节点:"+waitPath+"  系统时间:"+System.currentTimeMillis());
                    zk.getData(waitPath,true,new Stat());
                }
        }
    }
    }

    public static void main(String[] args) {
       /* for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {
                public void run() {
                    SimpleDistributeLock simpleDistributeLock = new SimpleDistributeLock();
                    simpleDistributeLock.connectZk();
                }
            }).start();
        }*/
        SimpleDistributeLock simpleDistributeLock = new SimpleDistributeLock();
        simpleDistributeLock.connectZk();

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
