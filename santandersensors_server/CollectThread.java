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


import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.json.JSONArray;
import org.json.JSONObject;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Date;
import java.util.Timer;
import java.net.URL;

/**
 *
 * @author giovanni
 */

public class CollectThread extends Thread{
    private MongoCredential cred;
    private ServerAddress address;
    private String db;
    private String lastdataColl;
    private String tagsColl;
    private String collection;
    private String  discardColl;
    private String  typeColl;
    private String URL;
    private MongoTools client;
    private JSONObject santanderJson;
    private JSONArray santanderSensorArray;
    private SimpleDateFormat dateFormat;
    private Calendar cal;
    private int lapse;
    private int[] sensorsStatistics={0,0,0};


    public CollectThread(JsonFile conf){
        try{
            this.cred = MongoCredential.createMongoCRCredential(conf.getJson().get("username").toString(),conf.getJson().get("userdatabase").toString(),conf.getJson().get("password").toString().toCharArray());
            this.address = new ServerAddress(conf.getJson().get("IP").toString());
            this.db = conf.getJson().get("maindatabase").toString();
            this.collection = conf.getJson().get("maincollection").toString();
            this.discardColl = conf.getJson().get("discardcollection").toString();
            this.typeColl = conf.getJson().get("typecollection").toString();
            this.lastdataColl = conf.getJson().get("lastdatacollection").toString();
            this.tagsColl = conf.getJson().get("tagscollection").toString();
            this.URL = conf.getJson().get("URL").toString();
            this.lapse = Integer.parseInt(conf.getJson().get("lapse").toString());
            //Creo la connessione a MongoDB
            this.client = new MongoTools(this.address, Arrays.asList(this.cred));
            this.client.setDatabase(this.db);
            this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.cal = new GregorianCalendar();
        }
        catch(Exception e){
            System.out.print("("+new GregorianCalendar().getTime()+") -> ");
            System.out.print("error: "+e+"\n");
        }
    }



    @Override
    public void run(){
        while(true){
            this.updateDate();
            try{
                //Raccolgo i dati dall'url
                JsonUtils santanderJsonRaw = new JsonUtils().read(new URL(this.URL));
                //Estraggo l'array e la lista dei tipi di sensori
                JSONArray santanderJsonArray = santanderJsonRaw.getArray("markers");
                this.santanderSensorArray = this.client.getJsonArrayOnceFromCollection(this.typeColl,"type");
                for(int i=0;i<santanderJsonArray.length();i++){
                    try{
                        //Per ogni stringa dell'array "markers" riformatto la stringa JSON
                        this.santanderJson = (JSONObject) santanderJsonArray.get(i);
                        this.reformatJsonString();
                        //Inserisco la stringa JSON modificata nelle collection del DB
                        if(!this.issueControl()){ //controllo eventuli problemi
                            this.client.insertInCollection(this.collection, this.santanderJson);
                            this.client.insertInCollection(this.lastdataColl, this.santanderJson);
                            this.sensorsStatistics[0]++;
                        }
                        //Inserisco le stringhe dei sensori con problemi nella collection degli scarti
                        else{
                            this.client.insertInCollection(this.discardColl, this.santanderJson);
                            this.sensorsStatistics[2]++;
                        }
                    }
                    catch(Exception e) {
                            this.sensorsStatistics[1]++;
                    }
                }
            //aggiorno la collection sulle informazioni dell'ultima raccolta.
            this.printStastics(); //stampo le informazioni sulla raccolta
            this.resetStatistics(); //cancello le informazioni sulla raccolta
            Thread.sleep(lapse*1000);
            this.client.removeFromCollection(this.lastdataColl, "{}");
            }
            catch(Exception e){
                System.out.print(e);
                break;
            }
        }
    }

    private boolean issueControl(){
        try{
            String jsonDate = this.santanderJson.get("Last update").toString();
            Date date = this.dateFormat.parse(jsonDate);
            if(date.before(this.cal.getTime())){
                return true;
            }
            if(Double.parseDouble(this.santanderJson.get("Battery level").toString())==0){
                return true;
            }
        }
        catch(Exception e){
            return false;
        }
        return false;
    }

    private void resetStatistics(){
    for(int i=0;i<this.sensorsStatistics.length;i++){sensorsStatistics[i]=0;}
    }
    private void updateDate(){
        this.cal = new GregorianCalendar();
        this.cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(new SimpleDateFormat("dd").format(this.cal.getTime()))-1);
    }

    private void printStastics(){
        Calendar cal = new GregorianCalendar();
        System.out.print("("+cal.getTime()+") -> ");
        System.out.print(" collected properly: "+this.sensorsStatistics[0]);
        System.out.print(" not working: "+this.sensorsStatistics[1]);
        System.out.print(" outdated: "+this.sensorsStatistics[2]+"\n");
    }

    private void reformatJsonString(){
        Double[] loc = {
            Double.parseDouble(this.santanderJson.get("longitude").toString()),
            Double.parseDouble(this.santanderJson.get("latitude").toString())
        };
        this.santanderJson.put("loc", loc);
        this.santanderJson.remove("longitude");
        this.santanderJson.remove("latitude");

        //leggo il campo content e lo cancello
        String content = this.santanderJson.get("content").toString();
        this.santanderJson.remove("content");
        ContentParser contentObj = new ContentParser(content);

        //Inserisco l'informazione last update se presente
        if(content.indexOf("Last update")> -1){
            this.santanderJson.put("Last update", contentObj.parseLastUpdate());
        }
        if(content.indexOf("Battery level")> -1){
            Double bat=Double.parseDouble(contentObj.parse("Battery level"));
            this.santanderJson.put("Battery level", bat);
        }

        //Aggiungo nella stringa JSON il tipo di sensore(se presente) e il relativo valore
        for(int j=0;j<this.santanderSensorArray.length();j++){

            //Cerco nel campo content ogni tipo di sensore presente nel DB
            String sensorType = this.santanderSensorArray.get(j).toString();
            if(content.indexOf(sensorType)> -1){
                String value = contentObj.parse(sensorType);
                //Aggiungo nella stringa JSON il tipo di sensore(se presente) e il relativo valore
                this.santanderJson.put(sensorType, Double.parseDouble(value));
            }
        }
    }
}
