package org.tjcj.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * 主要用于操作hdfs
 */
public class HDFSUtil {
    //文件系统
    private static FileSystem fileSystem;
    static{//静态代码块主要作用用于实例化fileSystem
        //获取当前的hadoop开发环境
        Configuration conf = new Configuration();
        //设置文件系统类型
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        if(fileSystem == null){
            try {
                fileSystem = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 创建一个文件夹方法
     * @param path
     */
    public static void createDir(String path) throws IOException {
        boolean flag = false;
        if (!fileSystem.exists(new Path(path))){//如果文件夹不存在
            flag = fileSystem.mkdirs(new Path(path));
        }
        if(flag){
            System.out.println("文件夹已经创建成功。。。");
        }else{
            System.out.println("文件夹已经存在。。。。");
        }
    }

    /**
     * 删除文件夹以及文件
     * @param path
     */
    public static void delete(String path) throws IOException {
        boolean flag = false;
        if(fileSystem.exists(new Path(path))){
            flag = fileSystem.delete(new Path(path),true);
        }
        if(flag){
            System.out.println("文件或者文件夹已经删除");
        }else{
            System.out.println("文件或者文件夹不存在");
        }
    }

    /**
     * 上传到hdfs上
     * @param srcPath
     * @param destPath
     * @throws IOException
     */
    public static void uploadFile(String srcPath,String destPath) throws IOException {
        fileSystem.copyFromLocalFile(false,true,new Path(srcPath),new Path(destPath));
        System.out.println("文件上传成功！！！");
    }
    public static void downloadFile(String srcPath,String destPath) throws IOException {
        fileSystem.copyToLocalFile(false,new Path(srcPath),new Path(destPath),true);
        System.out.println("文件下载成功");
    }
    public static void main(String[] args) throws IOException {
//        createDir("/javaMkdir");
//        delete("/javaMkdir");
        uploadFile("E:\\Code\\dsjsx_fzx\\mr\\input\\lol.txt","/javaMkdir");
//        downloadFile("/javaMkdir/hadoop.txt","E:\\Code\\dsjsx_fzx\\mr\\output\\");
    }
}