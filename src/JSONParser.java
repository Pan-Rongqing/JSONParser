import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;



import java.io.*;
import java.sql.SQLException;



public class JSONParser {
	public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException{
		String filepath="E:\\hurricaneharvey.json";
        parser(filepath);
	}
	
	 public static void parser (String filePath)throws IOException,SQLException,ClassNotFoundException{
	        File file =new File(filePath);
	        JSONObject[] jobj=new JSONObject[10000];
	        Store toStore=new Store();
	        int count=0;
	        if(!file.exists()){
	            System.out.println("Error:Can't find"+file);
	        }
	        /*BufferedReader in = new BufferedReader(new FileReader(file));
	        String line = "";
	        while (((line = in.readLine()) != null)) {
	            count++;


	        }
	        System.out.println(count);*/
	        try {
	            BufferedReader in = new BufferedReader(new FileReader(file));
	            String line = "";
	            StringBuilder str = new StringBuilder();
	            int i = 1;
	            for (i = 1; i <= 800; i++) {
	                count=0;
	                while (((line = in.readLine()) != null) && (count < 10000)) {
	                    jobj[count] = JSON.parseObject(line);
	                    count++;
	                }
	                //toStore.doStore_1(jobj);
	                //toStore.doStore_2(jobj);
	               toStore.doStore_3(jobj);
	               //toStore.doStore_4(jobj);
	            }
	            in.close();
	        }
	        catch(IOException e){
	            System.out.println("error");
	        }
	    }
}
