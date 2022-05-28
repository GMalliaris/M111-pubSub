import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class EchoServerForMultipleConnections {
  private static int PORT_NUMBER = 9001;
  private static int connection_count = 0;
  private static int request_count = 0;

  public static void main(String[] args) throws IOException {

    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    try {
      serverSocket = new ServerSocket(PORT_NUMBER);
    } catch (IOException exception) {
      exception.printStackTrace();
      System.out.println("Server Error");
    }
    while (true) {

      try {

        assert serverSocket != null;
        clientSocket = serverSocket.accept();
        System.out.println("Connection Established");
        new ServerThread(clientSocket, ++connection_count).start();
      } catch (IOException e) {
        System.out.println("Connection Error");
      } /*finally {
        if (serverSocket != null) {
          serverSocket.close();  // Do we need this?
        }
      }*/
    }
  }

  private static class ServerThread extends Thread {
    Socket clientSocket;
    int clientNum;

    public ServerThread(Socket clientSocket, int clientNum) {
      this.clientNum = clientNum;
      this.clientSocket = clientSocket;
      System.out.println("Creating connection for client #" + clientNum + " in a separate thread on the server");
    }

    @Override
    public void run() {
      PrintWriter clientOutStream = null;
      BufferedReader clientInStream = null;
      try {
        clientOutStream = new PrintWriter(clientSocket.getOutputStream(), true);

        clientInStream = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

        String inputLine;
        while ((inputLine = clientInStream.readLine()) != null) {
          Thread.sleep(1000);
          System.out.println("Server side message: Serving Client #" + clientNum + ". The request_count is " + request_count);
          clientOutStream.println(
              "Server Response# " + request_count++ + " - Hello  client #" + clientNum + "!! The message is: " + inputLine);
        }
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      } finally {
        try {
          clientSocket.close();
          assert clientOutStream != null;
          clientOutStream.close();
          assert clientInStream != null;
          clientInStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}