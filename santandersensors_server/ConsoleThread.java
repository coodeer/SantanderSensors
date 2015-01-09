/*
 * Copyright (C) 2014 Giovanni D'Italia
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package santandersensors_server;

import com.mongodb.DBCursor;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.DBObject;
import java.util.Arrays;
import org.json.JSONArray;
import org.json.JSONObject;
import java.net.*;
import java.io.*;
import java.util.GregorianCalendar;




/**
 *
 * @author giovanni
 */
public class ConsoleThread extends Thread{
    private MongoTools client;
    private MongoCredential cred;
    private ServerAddress address;
    private String db;
    private String tagsColl;
    private String collection;
    private MapReduce mapReduceOBJ;
    private int i;
    private String[] sensorvalue;
    private DBCursor info;
    private String  typeColl;
    private  String  discardColl;
    private JsonUtils infoArray;
    private BufferedReader input;
    private PrintWriter output;
    private Socket S;
    private int scelta;
    private String SensorName;
    private String Tags;
    private double SensorCutValue;
    private String[] SensorDateTime;
    private double[] CoordinateCenter;
    private double Radius;
    private String BUSTitle;
    private String ID;
    private int zErr=0;
    
    public ConsoleThread(JsonFile conf,Socket s){
        try{
            this.cred = MongoCredential.
                    createMongoCRCredential(
                            conf.getJson().get("username").toString(),
                            conf.getJson().get("userdatabase").toString(),
                            conf.getJson().get("password").toString()
                                    .toCharArray()
                    );
            this.address = new ServerAddress(
                    conf.getJson().get("IP").toString()
            );
            this.db = conf.getJson().get("maindatabase").toString();;
            this.collection = conf.getJson().get("maincollection").toString();
            this.discardColl = conf.getJson().get("discardcollection")
                    .toString();
            this.typeColl = conf.getJson().get("typecollection").toString(); 
            this.tagsColl = conf.getJson().get("tagscollection").toString();

            //Creo la connessione a MongoDB
            this.client = new MongoTools(
                    this.address, Arrays.asList(this.cred)
            );
            this.client.setDatabase(this.db);
            this.mapReduceOBJ = new MapReduce(this.client,this.collection);
            this.S = s;
        }
        catch(Exception e){
            this.interrupt();
        }
    }
    
    @Override
    public void run(){
        try{
            output = new PrintWriter(S.getOutputStream(), true);
            JSONArray tagsSensor = this.client.getJsonArrayOnceFromCollection(
                this.tagsColl,"tags");
            output.println(tagsSensor.toString());
            JSONArray typeSensor = this.client.getJsonArrayOnceFromCollection(
                this.typeColl,"type");
            output.println(typeSensor.toString());
        while(true){
            this.input = new BufferedReader(new InputStreamReader(S.getInputStream()));
            this.StreamParse(this.input.readLine());
                    switch(scelta){
                        case 1:
                            this.sensorvalue = this.mapReduceOBJ.
                                    getAverageValue(SensorName,
                                    Tags,ID,SensorCutValue);
                            this.sendResult();
                            break;
                        case 2:
                            this.sensorvalue = this.mapReduceOBJ.
                                    getAverageValue(SensorName,
                                    Tags,ID,
                                    SensorCutValue,
                                    SensorDateTime
                                    );
                            this.sendResult();
                            break;
                        case 3:
                            this.sensorvalue = this.mapReduceOBJ.
                                    getAverageValue(SensorName,
                                    Tags,
                                    SensorCutValue,
                                    CoordinateCenter,
                                    Radius
                                    );
                            this.sendResult();
                            break;
                        case 4:
                            this.sensorvalue = this.mapReduceOBJ.
                                    getAverageValue(SensorName,
                                    Tags,
                                    SensorCutValue,
                                    SensorDateTime,
                                    CoordinateCenter,
                                    Radius
                                    );
                            this.sendResult();
                            break;
                        case 5:
                            this.sensorvalue = this.mapReduceOBJ.
                                    getBUSInfo(ID,
                                    SensorDateTime
                                    );
                            this.sendResult();
                    }
        }
        
        }
        catch(Exception e){
            System.out.print("("+new GregorianCalendar().getTime()+") -> ");
            System.out.print(" fatal error: "+e+"\n");
            this.error();
        }
}
       
    private void sendResult(){
        if(this.sensorvalue.length<1){
            output.println("{}");
            return;
        }
        for(i=0;i<this.sensorvalue.length;i++){
            JSONObject str = new JSONObject(this.sensorvalue[i]);
            output.println(str);
        }   
    }
    
    private void StreamParse(String str){
        try{
        JsonUtils jsonStr = new JsonUtils(str);
        this.printLog(str);
        scelta = Integer.parseInt(jsonStr.getJson().get("scelta").toString());
        if(scelta<5 && scelta > 0){
            SensorName = jsonStr.getJson().get("name").toString();
            Tags = jsonStr.getJson().get("tags").toString();
            SensorCutValue = Double.parseDouble(jsonStr.getJson().get("cutValue")
                    .toString());
            ID = jsonStr.getJson().get("id").toString();
            if(scelta == 2 || scelta == 4 ){
                SensorDateTime = new String[2];
                SensorDateTime[0] = jsonStr.getArray("data").get(0).toString();
                SensorDateTime[1] = jsonStr.getArray("data").get(1).toString();
            }
            if(scelta == 3 || scelta == 4){
                CoordinateCenter = new double[2];
                CoordinateCenter[0] = Double.parseDouble(jsonStr.getArray("loc")
                    .get(0).toString());
                CoordinateCenter[1] = Double.parseDouble(jsonStr.getArray("loc")
                    .get(1).toString());
                Radius = Double.parseDouble(jsonStr.getJson().get("rad")
                        .toString()); 
            }
        }
        else{
            ID = jsonStr.getJson().get("id").toString();
            SensorDateTime = new String[2];
            SensorDateTime[0] = jsonStr.getArray("data").get(0).toString();
            SensorDateTime[1] = jsonStr.getArray("data").get(1).toString();
        }
        }
        catch(Exception e){
            System.out.print("("+new GregorianCalendar().getTime()+") -> ");
            System.out.print(" fatal error: "+e+"\n");
            this.error();
        }
        }
    
    private void error(){
        try{
        S.close();
        this.interrupt();
        }
        catch(Exception e){
        }
    }
    
    private void printLog(String str){
        System.out.print("("+new GregorianCalendar().getTime()+") -> ");
        System.out.print(S.getInetAddress()+" sends: ");
        System.out.print(str+"\n");
    }
}
