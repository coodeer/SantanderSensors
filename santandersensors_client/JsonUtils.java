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

package santandersensors_client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author giovanni
 */
public class JsonUtils extends JSONObject{
        protected JSONObject json = new JSONObject();
        
        public JsonUtils(){
         }
        
        public JsonUtils(JSONObject json){
        this.json=json;
        }
        
        public JsonUtils(String json){
        this.json= new JSONObject(json);
        }
    
        public JSONArray getArray(String arrayKey){
        JSONArray array = this.json.getJSONArray(arrayKey);
        return array;
        }
        
        public JSONObject getJson(){
        return this.json;
        }
        
        
        public JsonUtils read(URL url) throws Exception{
        BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
        String jsonTxt = IOUtils.toString(in);
            this.json = new JSONObject(jsonTxt);
        return this;
        }
        
}
