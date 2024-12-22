import java.io.*;
import java.net.*;
import java.util.*;

public class MainServer {
    private int port;
    private List<SubServerInfo> subServers;
    private static final int CHUNK_SIZE = 1024 * 1024; 
    private Properties config;

    public MainServer() {
        this.subServers = new ArrayList<>();
        loadConfiguration();
    }

    private void loadConfiguration() {
        config = new Properties();
        try (InputStream input = new FileInputStream("configuration.txt")) {
            config.load(input);
            
            this.port = Integer.parseInt(config.getProperty("port", "5000"));
            
            List<SubServerInfo> tempServers = new ArrayList<>();
            int serverIndex = 1;
            
            while (true) {
                String portKey = "slave.port" + serverIndex;
                String dirKey = "slave.directory." + serverIndex;
                
                String portValue = config.getProperty(portKey);
                String dirValue = config.getProperty(dirKey);
                
                if (portValue == null || dirValue == null) {
                    break; 
                }
                
                try {
                    int subServerPort = Integer.parseInt(portValue);
                    tempServers.add(new SubServerInfo("localhost", subServerPort, dirValue));
                    serverIndex++;
                } catch (NumberFormatException e) {
                    System.err.println("Port invalide pour " + portKey + ": " + portValue);
                }
            }
            
            if (tempServers.isEmpty()) {
                throw new IOException("Aucun sous-serveur configuré");
            }
            
            this.subServers = tempServers;
            System.out.println("Nombre de sous-serveurs configurés: " + subServers.size());
            
        } catch (IOException ex) {
            System.err.println("Erreur de lecture du fichier de configuration : " + ex.getMessage());
            this.port = 5000;
            subServers.add(new SubServerInfo("localhost", 5001, "server_1/"));
            System.out.println("Utilisation de la configuration par défaut");
        }
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Serveur principal démarré sur le port " + port);
            System.out.println("En attente de connexions...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Nouvelle connexion : " + clientSocket.getInetAddress());
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Erreur du serveur principal : " + e.getMessage());
            e.printStackTrace();
        }
    }

    private class ClientHandler implements Runnable {
        private Socket clientSocket;
        private DataInputStream in;
        private DataOutputStream out;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try {
                in = new DataInputStream(clientSocket.getInputStream());
                out = new DataOutputStream(clientSocket.getOutputStream());

                String operation = in.readUTF();
                if ("PING".equals(operation)) {
                    System.out.println("Connexion testée réussie.");
                    return;
                }

                switch (operation) {
                    case "UPLOAD":
                        handleUpload();
                        break;
                    case "DOWNLOAD":
                        handleDownload();
                        break;
                    case "LIST":
                        handleList();
                        break;
                    case "REMOVE":
                        handleRemove();
                        break;
                    default:
                        System.err.println("Opération inconnue : " + operation);
                }
            } catch (IOException e) {
                System.err.println("Erreur de communication : " + e.getMessage());
            } finally {
                try {
                    if (clientSocket != null) {
                        clientSocket.close();
                    }
                } catch (IOException e) {
                    System.err.println("Erreur lors de la fermeture de la connexion : " + e.getMessage());
                }
            }
        }

        private void handleUpload() throws IOException {
            String fileName = in.readUTF();
            long fileSize = in.readLong();

            File tempFile = File.createTempFile("upload", ".tmp");
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                byte[] buffer = new byte[CHUNK_SIZE];
                long remaining = fileSize;
                while (remaining > 0) {
                    int read = in.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                    if (read == -1) break;
                    fos.write(buffer, 0, read);
                    remaining -= read;
                }
            }

            try {
                distributeFile(tempFile, fileName);
                out.writeBoolean(true); 
                System.out.println("Fichier distribué avec succès : " + fileName);
            } catch (IOException e) {
                out.writeBoolean(false); 
                System.err.println("Erreur lors de la distribution du fichier : " + e.getMessage());
            } finally {
                tempFile.delete();
            }
        }

        private void handleDownload() throws IOException {
            String fileName = in.readUTF();
            
            File tempFile = File.createTempFile("download", ".tmp");
            boolean success = false;

            try (FileOutputStream fos = new FileOutputStream(tempFile)) {

                for (int i = 0; i < subServers.size(); i++) {
                    SubServerInfo subServer = subServers.get(i);
                    try (Socket subServerSocket = new Socket(subServer.getHost(), subServer.getPort())) {
                        DataInputStream subIn = new DataInputStream(subServerSocket.getInputStream());
                        DataOutputStream subOut = new DataOutputStream(subServerSocket.getOutputStream());

                        subOut.writeUTF("DOWNLOAD");
                        subOut.writeUTF(fileName + ".part" + (i + 1));

                        long chunkSize = subIn.readLong();
                        if (chunkSize == -1) {
                            out.writeLong(-1);
                            return;
                        }

                        byte[] buffer = new byte[CHUNK_SIZE];
                        long remaining = chunkSize;
                        while (remaining > 0) {
                            int read = subIn.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                            if (read == -1) break;
                            fos.write(buffer, 0, read);
                            remaining -= read;
                        }
                    }
                }
                success = true;
            } catch (IOException e) {
                System.err.println("Erreur lors de la reconstruction du fichier : " + e.getMessage());
                out.writeLong(-1);
                return;
            }

            if (success) {
                out.writeLong(tempFile.length());
                try (FileInputStream fis = new FileInputStream(tempFile)) {
                    byte[] buffer = new byte[CHUNK_SIZE];
                    int bytesRead;
                    while ((bytesRead = fis.read(buffer)) != -1) {
                        out.write(buffer, 0, bytesRead);
                    }
                }
                System.out.println("Fichier envoyé avec succès : " + fileName);
            }

            tempFile.delete();
        }

        private void handleList() throws IOException {
            Set<String> uniqueFiles = new HashSet<>();
            
            for (SubServerInfo subServer : subServers) {
                try (Socket subServerSocket = new Socket(subServer.getHost(), subServer.getPort())) {
                    DataInputStream subIn = new DataInputStream(subServerSocket.getInputStream());
                    DataOutputStream subOut = new DataOutputStream(subServerSocket.getOutputStream());

                    subOut.writeUTF("LIST");
                    int fileCount = subIn.readInt();

                    for (int i = 0; i < fileCount; i++) {
                        String fileName = subIn.readUTF();
                        fileName = fileName.replaceAll("\\.part[0-9]+$", "");
                        uniqueFiles.add(fileName);
                    }
                } catch (IOException e) {
                    System.err.println("Erreur lors de la lecture des fichiers du sous-serveur : " + e.getMessage());
                }
            }

            out.writeInt(uniqueFiles.size());
            for (String fileName : uniqueFiles) {
                out.writeUTF(fileName);
            }
        }

        private void handleRemove() throws IOException {
            String fileName = in.readUTF();
            boolean overallSuccess = true;

            for (int i = 0; i < subServers.size(); i++) {
                SubServerInfo subServer = subServers.get(i);
                try (Socket subServerSocket = new Socket(subServer.getHost(), subServer.getPort())) {
                    DataInputStream subIn = new DataInputStream(subServerSocket.getInputStream());
                    DataOutputStream subOut = new DataOutputStream(subServerSocket.getOutputStream());

                    subOut.writeUTF("REMOVE");
                    subOut.writeUTF(fileName + ".part" + (i + 1));

                    boolean success = subIn.readBoolean();
                    overallSuccess &= success;
                } catch (IOException e) {
                    System.err.println("Erreur lors de la suppression sur un sous-serveur : " + e.getMessage());
                    overallSuccess = false;
                }
            }

            out.writeBoolean(overallSuccess);
            if (overallSuccess) {
                System.out.println("Fichier supprimé avec succès : " + fileName);
            } else {
                System.err.println("Erreur lors de la suppression du fichier : " + fileName);
            }
        }

        private void distributeFile(File file, String fileName) throws IOException {
            long fileSize = file.length();
            int serverCount = subServers.size();
            long baseChunkSize = fileSize / serverCount;
            long remainingBytes = fileSize % serverCount;

            try (FileInputStream fis = new FileInputStream(file)) {
                for (int i = 0; i < serverCount; i++) {
                    SubServerInfo subServer = subServers.get(i);
                    try (Socket subServerSocket = new Socket(subServer.getHost(), subServer.getPort())) {
                        DataOutputStream subOut = new DataOutputStream(subServerSocket.getOutputStream());
                        
                        subOut.writeUTF("UPLOAD");
                        subOut.writeUTF(fileName + ".part" + (i + 1));
                        
                        long thisChunkSize = baseChunkSize;
                        if (i == serverCount - 1) {
                            thisChunkSize += remainingBytes;
                        }
                        subOut.writeLong(thisChunkSize);

                        byte[] buffer = new byte[CHUNK_SIZE];
                        long remaining = thisChunkSize;
                        while (remaining > 0) {
                            int read = fis.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                            if (read == -1) break;
                            subOut.write(buffer, 0, read);
                            remaining -= read;
                        }
                    }
                }
            }
        }
    }

    private static class SubServerInfo {
        private String host;
        private int port;
        private String directory;

        public SubServerInfo(String host, int port, String directory) {
            this.host = host;
            this.port = port;
            this.directory = directory;
        }

        public String getHost() { return host; }
        public int getPort() { return port; }
        public String getDirectory() { return directory; }
    }

    public static void main(String[] args) {
        MainServer server = new MainServer();
        server.start();
    }
}
