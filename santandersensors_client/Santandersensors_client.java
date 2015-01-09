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
import java.net.Socket;
import javax.swing.JOptionPane; 
/**
 *
 * @author giovanni
 */
public class Santandersensors_client {
    static JsonFile conf;
    static String address;
    static int port;
    static Socket s;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        try{
        conf = new JsonFile("config.json").read();
        address = conf.getJson().get("IP").toString();
        port = Integer.parseInt(conf.getJson().get("port").toString());
        s = new Socket(address, port);
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new JFrame(s).setVisible(true);
            }
        });
         }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.toString(), "Error",
                                    JOptionPane.ERROR_MESSAGE);
        System.exit(1);
        }
    }
    
}
