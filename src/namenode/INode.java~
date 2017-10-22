package namenode;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.logging.Level;
import com.sun.istack.internal.logging.Logger;
import com.sun.org.apache.xpath.internal.operations.Bool;

/**
 * Created by SAYAN on 15-10-2017.
 */
public class INode {

    String iNum;

    /*
     *  Index 0 - directory(d)/file(f)
     *  Index 1 - created date/time
     *  Index 3 - owner (sys/usr)
     */
    private String[] metadata;

    /*
     *  HashMap for storing pointers
     *  to the INodes.
     */
    private Map<String, INode> ls = new HashMap<>();

    /*  This stores the block numbers of the
     *  file.
     */
    private List<Integer> data = new ArrayList<>();

    /*
     *  Location of the folder that stores
     *  all the inodes.
     */
    static String SUPERBLOCK = "C:\\Users\\";

    private Boolean dirty = false;

    public INode(String iNum){
        this.iNum = iNum;
        readFromINode();
    }

    public INode(String iNum, String type, String created, String owner, Boolean isDirty){
        this.iNum = iNum;
        metadata = new String[3];
        this.metadata[0] = type;
        this.metadata[1] = created;
        this.metadata[2] = owner;
        this.dirty = isDirty;
    }

    public static void main(String[] args) {
        INode node = new INode("i1");
        String path = "usr/";
        System.out.println(node.insertIntoTree(node, "i4", "abc.txt", path.split("/"), "f"));

        node.printTree(node);

        System.out.println("Final print");
        INode t = node.getNode(node, ("usr/abc.txt").split("/"));

        System.out.println(t.metadata[0]);
        t.addBlockNo(2);
        t.addBlockNo(5);
        t.addBlockNo(6);

        System.out.println(t.getBlockNos());


    }

    public void printAnINode(INode node){
        System.out.println("Type : " + node.metadata[0]);

        System.out.println("Created : " + node.metadata[1]);
        System.out.println("Owner : " + node.metadata[2]);
        for (Map.Entry<String, INode> entry: node.ls.entrySet()){
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();
    }

    public void printTree(INode node){
        printAnINode(node);
        if(node.metadata[0].equals('f'))
            return;
        for (Map.Entry<String, INode> entry: node.ls.entrySet())
            printTree(entry.getValue());
    }

    public void readFromINode(){

        String s;
        try {
            BufferedReader br = new BufferedReader(
                    new FileReader(SUPERBLOCK + "\\" + this.iNum + ".txt"));
            this.metadata = br.readLine().split("\\|");

            if(this.metadata[0].equals("f")) {
                String[] blockNos;
                blockNos = br.readLine().split(",");
                for(int i = 0; i < blockNos.length; i++)
                    this.data.add(Integer.parseInt(blockNos[i]));
                //this.data = Arrays.asList(blockNo.split(","));
                //while( (blockNo = br.readLine()) != null)
                return;
            }

            while( (s = br.readLine()) != null){
                String[] line = s.split(" ");
                ls.put(line[0], new INode(line[1]));
            }
            br.close();
        }
        catch(Exception e){
            Logger.getLogger(this.getClass()).log(Level.SEVERE, "Something went wrong"
                    + " while reading from inode " + this.iNum + ".");
            e.printStackTrace();
        }
    }

    public boolean insertIntoTree(INode root, String inn, String file, String[] path, String type) {
        INode temp = root;
        for (int i = 0; i < path.length; i++) {
            if(path[i].equals(""))
                continue;
            if (temp.ls.containsKey(path[i]))
                temp = temp.ls.get(path[i]);
            else
                return false;
        }
        if (temp.metadata[0].equals("d")) {
            if(!temp.ls.containsKey(file))
                temp.ls.put(file, new INode(inn, type, (new Date()).toString(), "owner_id", true));
            else
                return false;
        } else
            return false;
        return true;
    }

    public boolean removeFromTree(INode root, String file, String[] path) {
        INode temp = root;
        for (int i = 0; i < path.length; i++) {
            if(path[i].equals(""))
                continue;
            if (temp.ls.containsKey(path[i])) {
                temp = temp.ls.get(path[i]);
            } else {
                return false;
            }
        }
        if (temp.metadata[0].equals("d")) {
            if(temp.ls.containsKey(file))
                temp.ls.remove(file);
            else
                return false;
        } else {
            return false;
        }
        return true;
    }

    public INode getNode(INode root, String[] path) {
        INode temp = root;
        for (int i = 0; i < path.length; i++) {
            if(path[i].equals(""))
                continue;
            if (temp.ls.containsKey(path[i]))
                temp = temp.ls.get(path[i]);
            else
                return null;
        }
        return temp;
    }

    public void addBlockNo(int block){
        this.data.add(block);
    }

    public String getBlockNos(){
        String ret = "";
        for(int i = 0; i < this.data.size(); i++)
            ret = ret + "," + Integer.toString(this.data.get(i));
        return ret.substring(1);
    }

    public String getINodeNo(){
        return this.iNum;
    }

    public Map<String, INode> getList(){
        return ls;
    }

    public String[] getMetadata(){
        return this.metadata;
    }

    public void setDirty(Boolean b){
        this.dirty = b;
    }

    public Boolean isDirty(){
        return this.dirty;
    }
}
