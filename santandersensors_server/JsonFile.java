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
import org.json.*;
import java.io.*;
import org.apache.commons.io.IOUtils;
/**
 *
 * @author giovanni
 */
public class JsonFile extends JsonUtils{
        private final String path;
        
        public JsonFile() throws Exception{
        this.path = "";
        }
        
        public JsonFile(String path) throws Exception{
        this.path = path;
        }
        
        
        public JsonFile read() throws Exception{
        File f = new File(this.path);
        if (f.exists()){
            InputStream is = new FileInputStream(this.path);
            String jsonTxt = IOUtils.toString(is);
            super.json = new JSONObject(jsonTxt);
         }
        return this;
        }
        
        
        public void write(JSONObject jsonStream) throws Exception{
            File f = new File(this.path);
            if (!f.exists()){
                f.createNewFile();
            }
            OutputStream is = new FileOutputStream(this.path);
            String jsonTxt = jsonStream.toString();
            IOUtils.write(jsonTxt, is);   
        }
        
        
  
}

