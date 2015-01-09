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

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author giovanni
 */
public class MapReduce {
    private final MongoClient client;
    private final String collection;
    private String map;
    private String reduce;
    
    public MapReduce(MongoClient client,String collection){
        this.client = client;
        this.collection = collection;
    }
    
    // <!>Specializzare i vari metodi<!>
    
    public String[] getInfoForTags(){     
        //LISTA TAGS:TITLE
        String map ="function () {"+
        "value = { info: [{'id':this.title,'loc' : this.loc}] };"+
        "emit(this.tags, value);"+
        "}";
 
        String reduce="function(key, values) {"+
        "title_list = { info: [] };"+
        "for(var i in values) {"+
        "title_list.info = values[i].info.concat(title_list.info);"+
        "}"+
        "return title_list;"+
        "}";
         
        List<String> stringValue = new ArrayList<String>();
        int i = 0;
        for(DBObject o : ((MongoTools)this.client).mongoMapReduce(this.collection,map, reduce,null).results())
        {
         stringValue.add(o.toString());
         i++;
        }
        String[] array = new String[stringValue.size()];
        stringValue.toArray(array);
        return array;
    }
    
    public String[] getAverageValue(String sensorType,String tags,String id,double range){   
        //MEDIA TAGS:SENROSTYPE con la query posso definire un periodo temporale e una zona geografica
        DBObject  query = (DBObject) JSON.parse("{}");
        if(!tags.equals("")){query.put("tags",tags);}
        if(!id.equals("")){query.put("title", id);}
        
        map ="function () {"+
        "emit(this.tags, this['"+sensorType+"']);"+
        "}";
        
        reduce="function(key, values) {"+
        "title_list =0;"+
        "j=0;"+
        "mid = values.length/2 | 0;"+
        "values = values.sort();"+
        "for(var i in values) {"+
        "if(values[i]>(values[mid]-"+range+") && values[i]<(values[mid]+"+range+")){"+
        "title_list += values[i];"+
        "j++}"+
        "}"+
        "return title_list/j;"+
        "}";
        
        List<String> stringValue = new ArrayList<String>();
        int i = 0;
        for(DBObject o : ((MongoTools)this.client).mongoMapReduce(this.collection,map, reduce,query).results())
        {
         stringValue.add(o.toString());
         i++;
        }
        String[] array = new String[stringValue.size()];
        stringValue.toArray(array);
        return array;
    }
    
    public String[] getAverageValue(String sensorType,String tags,String id,double range,String[] time){   
        //MEDIA TAGS:SENROSTYPE con la query posso definire un periodo temporale e una zona geografica
        DBObject  query = (DBObject) JSON.parse("{'Last update':{$gt:'"+time[0]+"',$lt:'"+time[1]+"'}}");
        if(!tags.equals("")){query.put("tags",tags);}
        if(!id.equals("")){query.put("title", id);}
        System.out.println(query.toString());
        
        map ="function () {"+
        "emit(this.tags, this['"+sensorType+"']);"+
        "}";
 
        reduce="function(key, values) {"+
        "title_list =0;"+
        "j=0;"+
        "mid = values.length/2 | 0;"+
        "values = values.sort();"+
        "for(var i in values) {"+
        "if(values[i]>(values[mid]-"+range+") && values[i]<(values[mid]+"+range+")){"+
        "title_list += values[i];"+
        "j++}"+
        "}"+
        "return title_list/j;"+
        "}";
        
        List<String> stringValue = new ArrayList<String>();
        int i = 0;
        for(DBObject o : ((MongoTools)this.client).mongoMapReduce(this.collection,map, reduce,query).results())
        {
         stringValue.add(o.toString());
         i++;
         
         
        }
        String[] array = new String[stringValue.size()];
        stringValue.toArray(array);
        return array;
    }
    
    public String[] getAverageValue(String sensorType,String tags,double range,double[] loc,double radius){   
        //MEDIA TAGS:SENROSTYPE con la query posso definire un periodo temporale e una zona geografica
        DBObject  query;
        if(!tags.equals("")){
        query = (DBObject) JSON.parse("{loc: { $geoWithin: { $center: [["+loc[0]+","+loc[1]+"],"+radius+"] } },'tags':'"+tags+"'}");
        }
        else{
        query = (DBObject) JSON.parse("{loc: { $geoWithin: { $center: [["+loc[0]+","+loc[1]+"],"+radius+"] } }}");
        }
        
        map ="function () {"+
        "emit(this.tags, this['"+sensorType+"']);"+
        "}";
 
        reduce="function(key, values) {"+
        "title_list =0;"+
        "j=0;"+
        "mid = values.length/2 | 0;"+
        "values = values.sort();"+
        "for(var i in values) {"+
        "if(values[i]>(values[mid]-"+range+") && values[i]<(values[mid]+"+range+")){"+
        "title_list += values[i];"+
        "j++}"+
        "}"+
        "return title_list/j;"+
        "}";
        
        List<String> stringValue = new ArrayList<String>();
        int i = 0;
        for(DBObject o : ((MongoTools)this.client).mongoMapReduce(this.collection,map, reduce,query).results())// <!>
        {
         stringValue.add(o.toString());
         i++;
        }
        String[] array = new String[stringValue.size()];
        stringValue.toArray(array);
        return array;
    }

    public String[] getAverageValue(String sensorType,String tags,double range,String[] time,double[] loc,double radius){   
        //MEDIA TAGS:SENROSTYPE con la query posso definire un periodo temporale e una zona geografica
        DBObject  query;
        if(!tags.equals("")){
        query = (DBObject) JSON.parse("{'Last update':{$gt:'"+time[0]+"',$lt:'"+time[1]+"'},loc: { $geoWithin: { $center: [["+loc[0]+","+loc[1]+"],"+radius+"] } },'tags':'"+tags+"'}");
        }
        else{
        query = (DBObject) JSON.parse("{'Last update':{$gt:'"+time[0]+"',$lt:'"+time[1]+"'},loc: { $geoWithin: { $center: [["+loc[0]+","+loc[1]+"],"+radius+"] } }}");
        }
        
        map ="function () {"+
        "emit(this.tags, this['"+sensorType+"']);"+
        "}";
 
        reduce="function(key, values) {"+
        "title_list =0;"+
        "j=0;"+
        "mid = values.length/2 | 0;"+
        "values = values.sort();"+
        "for(var i in values) {"+
        "if(values[mid]-"+range+"<values[i] && values[mid]+"+range+">values[i]){"+
        "title_list += values[i];"+
        "j++}"+
        "}"+
        "return title_list/j;"+
        "}";
        
        List<String> stringValue = new ArrayList<String>();
        int i = 0;
        for(DBObject o : ((MongoTools)this.client).mongoMapReduce(this.collection,map, reduce,query).results())
        {
         stringValue.add(o.toString());
         i++;
        }
        String[] array = new String[stringValue.size()];
        stringValue.toArray(array);
        return array;
    }
    
    public String[] getBUSInfo(String title,String[] time){     
        //LISTA TAGS:TITLE
        String map ="function () {"+
        "value =  {loc : [this.loc]} ;"+
        "emit(this.title, value);"+
        "}";
 
        String reduce="function(key, values) {"+
        "title_list = { loc: [] };"+
        "for(var i in values) {"+
        "title_list.loc = values[i].loc.concat(title_list.loc);"+
        "}"+
        "return title_list;"+
        "}";
        
        DBObject  query = (DBObject) JSON.parse("{'Last update':{$gt:'"+time[0]+"',$lt:'"+time[1]+"'},'title':'"+title+"'}");
        List<String> stringValue = new ArrayList<String>();
        int i = 0;
        for(DBObject o : ((MongoTools)this.client).mongoMapReduce(this.collection,map, reduce,query).results())
        {
         stringValue.add(o.toString());
         i++;
        }
        String[] array = new String[stringValue.size()];
        stringValue.toArray(array);
        return array;
    }
}
