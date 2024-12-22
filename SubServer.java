import java.io.*;
import java.net.*;
import java.util.*;

public class SubServer {
    private int port;
    private String storageDirectory;
    private Properties config;
    private static final int CHUNK_SIZE = 1024 * 1024; 

    public SubServer(int serverNumber) {
        loadConfiguration(serverNumber);
        createStorageDirectory();
    }

    private void loadConfiguration(int serverNumber) {
        config = new Properties();
        try (InputStream input = new FileInputStream("configuration.txt")) {
            config.load(input);
            
            String portKey = "slave.port" + serverNumber;
            String dirKey = "slave.directory." + serverNumber;
            
            String portValue = config.getProperty(portKey);
            String dirValue = config.getProperty(dirKey);
            
            if (portValue == null || dirValue == null) {
                throw new IllegalArgumentException("Configuration non trouvée pour le serveur " + serverNumber);
            }
            
            this.port = Integer.parseInt(portValue);
            this.storageDirectory = dirValue;
            
        } catch (IOException ex) {
            System.err.println("Erreur de lecture du fichier de configuration : " + ex.getMessage());
            this.port = 5000 + serverNumber;
            this.storageDirectory = "server_" + serverNumber + "/";
        }
    }

    private void createStorageDirectory() {
        File directory = new File(storageDirectory);
        if (!directory.exists()) {
            if (directory.mkdirs()) {
                System.out.println("Répertoire de stockage créé : " + storageDirectory);
            } else {
                System.err.println("Impossible de créer le répertoire de stockage : " + storageDirectory);
            }
        }
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Sous-serveur démarré sur le port " + port);
            System.out.println("Répertoire de stockage : " + storageDirectory);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Nouvelle connexion du serveur principal : " + clientSocket.getInetAddress());
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Erreur du sous-serveur : " + e.getMessage());
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

            File file = new File(storageDirectory + File.separator + fileName);
            file.getParentFile().mkdirs();

            try (FileOutputStream fos = new FileOutputStream(file);
                 BufferedOutputStream bos = new BufferedOutputStream(fos)) {

                byte[] buffer = new byte[CHUNK_SIZE];
                long remaining = fileSize;
                while (remaining > 0) {
                    int read = in.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                    if (read == -1) break;
                    bos.write(buffer, 0, read);
                    remaining -= read;
                }
            }
            System.out.println("Fichier reçu : " + fileName);
        }

        private void handleDownload() throws IOException {
            String fileName = in.readUTF();
            File file = new File(storageDirectory + File.separator + fileName);

            if (!file.exists()) {
                out.writeLong(-1);
                System.err.println("Fichier non trouvé : " + fileName);
                return;
            }

            out.writeLong(file.length());

            try (FileInputStream fis = new FileInputStream(file);
                 BufferedInputStream bis = new BufferedInputStream(fis)) {

                byte[] buffer = new byte[CHUNK_SIZE];
                int bytesRead;
                while ((bytesRead = bis.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
            }
            System.out.println("Fichier envoyé : " + fileName);
        }

        private void handleList() throws IOException {
            File directory = new File(storageDirectory);
            File[] files = directory.listFiles();

            if (files == null) {
                out.writeInt(0);
                return;
            }

            List<File> fileList = new ArrayList<>();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {  
                        fileList.add(file);
                    }
                }
            }

            out.writeInt(fileList.size());
            for (File file : fileList) {
                out.writeUTF(file.getName());
            }
            System.out.println("Liste des fichiers envoyée, " + fileList.size() + " fichiers trouvés");
        }

        private void handleRemove() throws IOException {
            String fileName = in.readUTF();
            File file = new File(storageDirectory + File.separator + fileName);

            boolean success = file.exists() && file.delete();
            out.writeBoolean(success);

            if (success) {
                System.out.println("Fichier supprimé : " + fileName);
            } else {
                System.err.println("Échec de la suppression du fichier : " + fileName);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java SubServer <serverNumber>");
            System.out.println("serverNumber doit être un nombre positif correspondant à la configuration");
            return;
        }
        
        try {
            int serverNumber = Integer.parseInt(args[0]);
            if (serverNumber < 1) {
                throw new IllegalArgumentException("Le numéro de serveur doit être positif");
            }
            
            SubServer server = new SubServer(serverNumber);
            server.start();
        } catch (NumberFormatException e) {
            System.out.println("Le numéro de serveur doit être un nombre");
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
    }
}
