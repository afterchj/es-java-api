package com.after.es;

import java.io.*;

/**
 * @author hongjian.chen
 * @date 2019/12/11 17:47
 */
public class MainTest {


    public static String readJsonFile(String fileName) {
        String jsonStr = "";
        try {
            File jsonFile = new File(fileName);
            FileReader fileReader = new FileReader(jsonFile);

            Reader reader = new InputStreamReader(new FileInputStream(jsonFile),"utf-8");
            int ch = 0;
            StringBuffer sb = new StringBuffer();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        String path =new MainTest().getClass().getClassLoader().getResource("mapping.json").getPath();
        String s = readJsonFile(path);
        System.out.println("path=" + path+ ",mappingJson=" + s);
    }
}
