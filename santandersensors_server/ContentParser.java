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
/**
 *
 * @author giovanni
 */
public class ContentParser {
    private final String content;
    
    public ContentParser(String content){
        this.content=content;
    }
    
    public String parse(String string){
        int start = content.indexOf(string);
        start = start+string.length()+2;
        return this.parse(start);
    }
    
    
    public String parse(int start){
        StringBuffer out = new StringBuffer();
        for(int i=0;content.charAt(i+start)!=' ' && content.charAt(i+start)!='%'&& content.charAt(i+start)!='<' ;i++){
            out.insert(i,content.charAt(i+start));
        }
        return out.toString();
    }
    
    public String parseLastUpdate(){
    int start = content.indexOf("Last update");
    start = start+"Last update".length()+2;
    StringBuffer out = new StringBuffer();
        for(int i=0;content.charAt(i+start)!='<' ;i++){
            out.insert(i,content.charAt(i+start));
        }
        return out.toString();
    
    }
            
}
