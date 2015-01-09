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
import com.mongodb.*;
import java.net.*;
import java.util.Arrays;
import org.json.*;
import java.io.*;
import java.util.GregorianCalendar;

/**
 *
 * @author giovanni
 */
public class Santandersensors_server {
    static JsonFile conf;
    static Thread collection , server;
    static ServerSocket s;
    static Socket sock;
    static int port;
    static String address;
    /**
     * @param args the command line arguments
     */
    
    public static void main(String[] args) throws Exception{
        try{
        conf = new JsonFile("config.json").read();
        address = conf.getJson().get("bind_IP").toString();
        port = Integer.parseInt(conf.getJson().get("port").toString());
        collection = new CollectThread(conf);
        collection.start();
        s = new ServerSocket(port,50,InetAddress.getByName(address));
        System.out.print("("+new GregorianCalendar().getTime()+") -> ");
        System.out.print("listening on: "+ address+":"+port+"\n");
        }
         catch(Exception e){
            System.out.print("("+new GregorianCalendar().getTime()+") -> ");
            System.out.print("error: "+e);
        }
        while(true){
        try{
        sock = s.accept();
        System.out.print("("+new GregorianCalendar().getTime()+") -> ");
        System.out.print("connection from "+sock.getInetAddress()+":");
        System.out.print(sock.getPort()+"\n");
        server = new ConsoleThread(conf,sock);
        server.start();
        }
        catch(Exception e){
            System.out.print("("+new GregorianCalendar().getTime()+") -> ");
            System.out.print("error: "+e);
            continue;
        }
        }
    }
}
   
