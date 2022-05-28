import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class EchoClient {
    private final static String HOST_NAME = "localhost";
    private static int PORT_NUMBER = 9001;
    public static void main(String[] args) throws IOException {
        try (
               // Create Socket
               Socket echoSocket = new Socket(HOST_NAME, PORT_NUMBER, InetAddress.getLocalHost(), 9004);

               // Create Socket in and out streams
               PrintWriter socketOutStream =
                new PrintWriter(echoSocket.getOutputStream(), true);
               BufferedReader socketInStream =
                new BufferedReader(
                    new InputStreamReader(echoSocket.getInputStream()));

               // Create System in stream to let the user type a message
               BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in))
        ) {
           String userInput;  // User types this in the console on client side
            while ((userInput = stdIn.readLine()) != null) {
                socketOutStream.println(userInput);  // Send it to the server
                System.out.println("On port: " + echoSocket.getLocalPort());
                System.out.println("Received: " + socketInStream.readLine()); //from server
            }
        } catch (UnknownHostException e) {
            //brevity
           e.printStackTrace();
       } catch (IOException e) {
            //brevity
           e.printStackTrace();
        } 
    }
}