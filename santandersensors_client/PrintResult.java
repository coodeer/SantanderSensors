/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package santandersensors_client;
import java.io.BufferedReader;
import java.net.*;
import org.json.JSONObject;
import java.text.DecimalFormat;
/**
 *
 * @author inna
 */
public class PrintResult extends Thread{
    final private javax.swing.JTextArea text;
    private Socket s;
    final private BufferedReader input;
    private String rcv;
    private JSONObject str;
    
    
    public PrintResult(javax.swing.JTextArea text,BufferedReader input){
        this.input = input;
        this.text = text;
    }
    
    public void run(){
        
        while(true){
          try{
          rcv = this.input.readLine();
          text.setForeground(java.awt.Color.BLACK);
          str = new JSONObject(rcv);
          //System.out.print(str);
          if(Double.parseDouble(str.get("value").toString())!=0.00){
              if(!this.text.getText().toString().equals("In attesa di risposta..."))
                this.text.append(str.get("_id")+" : "+new DecimalFormat("0.00").format(Double.parseDouble(str.get("value")
                  .toString()))+"\n");
              else
                this.text.setText(str.get("_id")+" : "+new DecimalFormat("0.00").format(Double.parseDouble(str.get("value")
                  .toString()))+"\n");
              
          }
          else{
              if(!this.text.getText().toString().equals("In attesa di risposta..."))
              this.text.append(str.get("_id")+" : 0,00"+"\n");
              else
                this.text.setText(str.get("_id")+" : 0,00"+"\n");
          }
        }
          catch(Exception e){
            if(!this.text.getText().toString().equals("In attesa di risposta..."))
                this.text.append(str.toString()+"\n");
            else
                this.text.setText(str.toString()+"\n");
            continue;
        }
        }
        
    }
}
