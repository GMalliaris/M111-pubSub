import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Playground {

//    client
    void writeToSocket(int brokerPort, String brokerIp) {
        try (var s = new Socket(brokerIp, brokerPort);
             var dout = new DataOutputStream(s.getOutputStream())){

            dout.writeUTF("Hello Server");
            dout.flush();
        }
        catch(Exception e) {
            System.out.println(e);
        }
    }

//    server
    void readFromSocket(int pubPort){

        try (var ss = new ServerSocket(pubPort);
             var s = ss.accept();//establishes connection
             var dis = new DataInputStream(s.getInputStream())){

            var str= dis.readUTF();
            System.out.println("message: " + str);
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }
}
