import java.io.*;
import java.net.*;
import java.util.*;

public class Client {
    private int port;
    private String serverAddress;
    private String downloadDirectory;
    private Scanner scanner;

    public Client() {
        scanner = new Scanner(System.in);
        loadConfiguration();
        createDownloadDirectory();
    }

    public boolean promptServerConnection() {
    System.out.println("\n--- Connexion au Serveur ---");
    System.out.print("Entrez l' IP du serveur: ");
    String inputAddress = scanner.nextLine();
    serverAddress = inputAddress.isEmpty() ? "localhost" : inputAddress;

    while (true) {
        System.out.print("Entrez le port du serveur: ");
        String inputPort = scanner.nextLine();
        try {
            port = inputPort.isEmpty() ? 5000 : Integer.parseInt(inputPort);
            
            // Teste la connexion en envoyant "PING"
            try (Socket testSocket = new Socket(serverAddress, port);
                 DataOutputStream out = new DataOutputStream(testSocket.getOutputStream())) {
                out.writeUTF("PING");
                System.out.println("Connexion configurée : " + serverAddress + ":" + port);
                return true;
            } catch (ConnectException e) {
                System.err.println("Erreur de connexion : " + e.getMessage());
            } catch (IOException e) {
                System.err.println("Erreur d'E/S : " + e.getMessage());
            }
        } catch (NumberFormatException e) {
            System.out.println("Port invalide. Veuillez entrer un numéro de port valide.");
        }
    }
}


    public void loadConfiguration() {
        try {
            Properties prop = new Properties();
            try (InputStream input = new FileInputStream("configuration.txt")) {
                prop.load(input);
                downloadDirectory = prop.getProperty("client.download.directory", "client_downloads/");
            }
        } catch (IOException ex) {
            System.err.println("Erreur de lecture du fichier de configuration : " + ex.getMessage());
            downloadDirectory = "client_downloads/";
        }
    }

    public void createDownloadDirectory() {
        File downloadDir = new File(downloadDirectory);
        if (!downloadDir.exists()) {
            downloadDir.mkdirs();
        }
    }

    public void start() {
        // Première tentative de connexion
        if (!promptServerConnection()) {
            System.out.println("Fermeture du client.");
            return;
        }

        while (true) {
            System.out.println("\n--- Menu de Transfert de Fichiers ---");
            System.out.println("1. LIST - Voir les fichiers disponibles");
            System.out.println("2. UPLOAD - Envoyer un fichier");
            System.out.println("3. DOWNLOAD - Télécharger un fichier");
            System.out.println("4. REMOVE - Supprimer un fichier");
            System.out.println("5. Reconnecter à un autre serveur");
            System.out.println("6. Quitter");
            System.out.print("Choisissez une option : ");

            int choice = scanner.nextInt();
            scanner.nextLine();

            try {
                switch (choice) {
                    case 1:
                        listFiles();
                        break;
                    case 2:
                        System.out.print("Entrez le chemin absolu du fichier à uploader : ");
                        String uploadPath = scanner.nextLine();
                        uploadFile(uploadPath);
                        break;
                    case 3:
                        System.out.print("Entrez le nom du fichier à télécharger : ");
                        String fileName = scanner.nextLine();
                        downloadFile(fileName);
                        break;
                    case 4:
                        System.out.print("Entrez le nom du fichier à supprimer : ");
                        String removeFileName = scanner.nextLine();
                        removeFile(removeFileName);
                        break;
                    case 5:
                        if (!promptServerConnection()) {
                            System.out.println("Retour au menu principal.");
                        }
                        break;
                    case 6:
                        System.out.println("Au revoir !");
                        return;
                    default:
                        System.out.println("Option invalide !");
                }
            } catch (Exception e) {
                System.err.println("Erreur : " + e.getMessage());
            }
        }
    }

    public void listFiles() throws IOException {
        try (Socket socket = new Socket(serverAddress, port)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            out.writeUTF("LIST");

            int fileCount = in.readInt();
            System.out.println("\nFichiers disponibles :");
            for (int i = 0; i < fileCount; i++) {
                System.out.println(in.readUTF());
            }
        }
    }

    public void uploadFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            System.err.println("Le fichier n'existe pas : " + filePath);
            return;
        }

        try (Socket socket = new Socket(serverAddress, port)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            out.writeUTF("UPLOAD");
            out.writeUTF(file.getName());
            out.writeLong(file.length());

            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
            }
            System.out.println("Fichier uploadé avec succès : " + file.getName());
        }
    }

    public void downloadFile(String fileName) throws IOException {
        try (Socket socket = new Socket(serverAddress, port)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            out.writeUTF("DOWNLOAD");
            out.writeUTF(fileName);

            long fileSize = in.readLong();
            if (fileSize == -1) {
                System.err.println("Le fichier n'existe pas sur le serveur.");
                return;
            }

            File downloadFile = new File(downloadDirectory + fileName);

            try (FileOutputStream fos = new FileOutputStream(downloadFile);
                 BufferedOutputStream bos = new BufferedOutputStream(fos)) {

                byte[] buffer = new byte[4096];
                int bytesRead;
                long totalBytesRead = 0;

                while (totalBytesRead < fileSize &&
                        (bytesRead = in.read(buffer, 0,
                                (int) Math.min(buffer.length, fileSize - totalBytesRead))) != -1) {
                    bos.write(buffer, 0, bytesRead);
                    totalBytesRead += bytesRead;
                }
            }
            System.out.println("Fichier téléchargé avec succès : " + fileName);
        }
    }

    public void removeFile(String fileName) throws IOException {
        try (Socket socket = new Socket(serverAddress, port)) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            out.writeUTF("REMOVE");
            out.writeUTF(fileName);

            boolean success = in.readBoolean();
            if (success) {
                System.out.println("Fichier supprimé avec succès : " + fileName);
            } else {
                System.err.println("Échec de la suppression du fichier : " + fileName);
            }
        }
    }

    public static void main(String[] args) {
        Client client = new Client();
        client.start();
    }
}