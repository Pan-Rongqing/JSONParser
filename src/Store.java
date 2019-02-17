

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.sql.*;

import java.math.BigDecimal;
import java.util.Set;

public class Store {
    private String insert_sql_1;
    private String insert_sql_2;
    private String insert_sql_3;
    private String insert_sql_4;

    private String charset;
    private boolean debug;

    private  String connectStr;
    private  String username;
    private  String password;

    public Store(){
        connectStr="jdbc:mysql://localhost:3306/hurricaneharvey?characterEncoding=utf-8";
        insert_sql_1="INSERT INTO hashtags(tweet_id,hashtag_text) VALUES(?,?)";
        insert_sql_2="INSERT INTO mentions(tweet_id,user_id,mention_id,name,screen_name) VALUES(?,?,?,?,?)";
        insert_sql_3="INSERT INTO tweets(tweet_id,created_at,text,user_id,source,in_reply_to_status_id,in_reply_to_user_id,is_quote_status,retweeted_original_tweet_id,retweeted_original_user_id,retweet_count,favorite_count,lang)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        insert_sql_4="INSERT INTO users(user_id,tweet_id,name,location,description,verified,followers_count,friends_count,listed_count,favourites_count,statuses_count,created_at)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
        charset ="utf8";
        debug=true;
        username="root";
        password="781256";
    }


    public void doStore_1(JSONObject[] jobj) throws ClassNotFoundException, SQLException, IOException,BatchUpdateException {
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(connectStr, username,password);
        conn.setAutoCommit(false); // 设置手动提交
        int count = 0;
        int max=jobj.length;
        PreparedStatement psts = conn.prepareStatement(insert_sql_1);
        try{
            while (count<max) {
                if (debug) {
                    //      System.out.println();
                }
                psts.setString(1, jobj[count].getString("id_str"));
                JSONArray hashtags=(JSONArray)((JSONObject)(jobj[count].get("entities"))).get("hashtags");
                String text="";
                int i=0;
                for (i=0;i<hashtags.size();i++){
                    text=text+"#"+hashtags.getJSONObject(i).getString("text");
                }
                psts.setString(2,text);
                psts.addBatch();          // 加入批量处理
                count++;
            }
            psts.executeBatch(); // 执行批量处理
            conn.commit();  // 提交
            System.out.println("All down : " + count);
            conn.close();
        }catch (BatchUpdateException e){
            System.out.println("1error");
        }

    }

    public void doStore_2(JSONObject[] jobj) throws ClassNotFoundException, SQLException, IOException {
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(connectStr, username,password);
        conn.setAutoCommit(false); // 设置手动提交
        int count = 0;
        int max=jobj.length;
        PreparedStatement psts = conn.prepareStatement(insert_sql_2);
        while (count<max) {
            if (debug) {
            //    System.out.println();
            }
            JSONArray jarr=((JSONArray)((JSONObject)jobj[count].get("entities")).get("user_mentions"));

            psts.setString(1, jobj[count].getString("id_str"));
            psts.setLong(2, ((JSONObject)jobj[count].get("user")).getInteger("id"));
            psts.setLong(3,jarr.size()!=0?jarr.getJSONObject(0).getLong("id"):0);
            psts.setString(4,jobj[count].getString("name"));
            psts.setString(5,jarr.size()!=0?jarr.getJSONObject(0).getString("screen_name"):null);

            psts.addBatch();          // 加入批量处理
            count++;
        }
        psts.executeBatch(); // 执行批量处理
        conn.commit();  // 提交
        System.out.println("All down : " + count);
        conn.close();
    }

    public void doStore_3(JSONObject[] jobj) throws ClassNotFoundException, SQLException, IOException ,NullPointerException{
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(connectStr, username,password);
        conn.setAutoCommit(false); // 设置手动提交.replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", "")
        int count = 0;
        int max=jobj.length;

        PreparedStatement psts = conn.prepareStatement(insert_sql_3);
        while (count<max) {
            if (debug) {
                //System.out.println();
            }
            psts.setString(1,jobj[count].getString("id_str"));
            psts.setString(2,jobj[count].getString("created_at"));
            psts.setString(3,jobj[count].getString("text"));
            psts.setLong(4,((JSONObject)jobj[count].get("user")).getLong("id"));
            psts.setString(5,jobj[count].getString("source"));
            psts.setString(6,jobj[count].getString("in_reply_to_status_id"));
            //jobj[count].containsKey("in_reply_to_user_id")?jobj[count].getLong("in_reply_to_user_id"):null:
            //Set set =jobj[count].keySet();

            psts.setString(7,jobj[count].containsKey("in_reply_to_user_id")?jobj[count].getString("in_reply_to_user_id"):null);
            psts.setInt(8,jobj[count].containsKey("is_quote_status")?(jobj[count].getBoolean("is_quote_status")==true?1:0):null);
            //((JSONObject)jobj[count].get("retweeted_status")).getString("id_str")
            psts.setString(9,((JSONObject)jobj[count].get("retweeted_status")).containsKey("id_str")?((JSONObject)jobj[count].get("retweeted_status")).getString("id_str"):null);
            //Long.parseLong(((JSONObject)(((JSONObject)jobj[count].get("retweeted_status")).get("user"))).getString("id_str"))
            psts.setLong(10,Long.parseLong(((JSONObject)(((JSONObject)jobj[count].get("retweeted_status")).get("user"))).getString("id_str")));
            psts.setInt(11,jobj[count].getInteger("retweet_count"));
            psts.setInt(12,jobj[count].getInteger("favorite_count"));
            psts.setString(13,jobj[count].getString("lang"));

            psts.addBatch();          // 加入批量处理
            count++;
        }
        psts.executeBatch(); // 执行批量处理
        conn.commit();  // 提交
        System.out.println("All down : " + count);
        conn.close();
    }

    public void doStore_4(JSONObject[] jobj) throws ClassNotFoundException, SQLException, IOException {
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(connectStr, username,password);
        conn.setAutoCommit(false); // 设置手动提交
        int count = 0;
        int max=jobj.length;
        PreparedStatement psts = conn.prepareStatement(insert_sql_4);
        while (count<max) {
            if (debug) {
                //System.out.println();
            }
            psts.setLong(1,((JSONObject)jobj[count].get("user")).getLong("id"));
            psts.setString(2,jobj[count].getString("id_str"));
            psts.setString(3,((JSONObject)jobj[count].get("user")).getString("name").replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", ""));
            psts.setString(4,((JSONObject)jobj[count].get("user")).getString("location").replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", ""));
            psts.setString(5,((JSONObject)jobj[count].get("user")).getString("description").replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", ""));
            psts.setInt(6,((JSONObject)jobj[count].get("user")).getBoolean("verified")==true?1:0);
            psts.setInt(7,((JSONObject)jobj[count].get("user")).getInteger("followers_count"));
            psts.setInt(8,((JSONObject)jobj[count].get("user")).getInteger("friends_count"));
            psts.setInt(9,((JSONObject)jobj[count].get("user")).getInteger("listed_count"));
            psts.setInt(10,((JSONObject)jobj[count].get("user")).getInteger("favourites_count"));
            psts.setInt(11,((JSONObject)jobj[count].get("user")).getInteger("statuses_count"));
            psts.setString(12,jobj[count].getString("created_at").replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", ""));
            psts.addBatch();          // 加入批量处理
            count++;
        }
        psts.executeBatch(); // 执行批量处理
        conn.commit();  // 提交
        System.out.println("All down : " + count);
        conn.close();
    }
}