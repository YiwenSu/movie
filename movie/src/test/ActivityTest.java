package test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.codehaus.jettison.json.JSONArray;

import com.alibaba.fastjson.JSON;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.to.ActivityTO;

public class ActivityTest {

	public static void main(String[] args) throws Exception {
		ActivityTest activityTest = new ActivityTest();
		activityTest.uploadProfile();
	}
	public void uploadProfile() throws IOException {
        FileReader fr = null;
        ActivityBean activityBean;
        try {
                        
            /**
             * Open the customer.out file for read.
             */
            fr = new FileReader(Constant.ACTIVITY_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String jsonTxt = null;
            //String password = StringUtil.getMessageDigest(Constant.DEMO_PASSWORD);
            int count = 1;
            
            /**
             * Loop through the file until EOF. Save the content of each row in
             * the jsonTxt string.
             */
            while ((jsonTxt = br.readLine()) != null) {
                
                if (jsonTxt.trim().length() == 0)
                    continue;
                
                try {
                	activityBean = new ActivityBean();
                	//JSON.parse(jsonTxt.trim());
                	activityBean = JSON.parseObject(jsonTxt.trim(), ActivityBean.class);
                    if(activityBean.getCustid()>0&&activityBean.getMovieid()>0&&activityBean.getRating()>0) {
                    	System.out.println(activityBean.getCustid()+","+activityBean.getMovieid()+","+activityBean.getRating());
                    	count++;
                    }
                	
                } catch (Exception e) {
                    System.out.println("ERROR: Not able to parse the json string: \t" +
                                       jsonTxt);
                }
            } //EOF while
            System.out.println(count);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }
    } //uploadProfile
}
