package nosql.workshop.batch.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import java.io.*;

public class EquipementsImporter {

    private final DBCollection installationsCollection;

    public EquipementsImporter(DBCollection installationsCollection) {
        this.installationsCollection = installationsCollection;
    }

    public void run() {
        InputStream is = CsvToMongoDb.class.getResourceAsStream("/csv/equipements.csv");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .forEach(line -> updateInstallation(line));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void updateInstallation(final String line) {
        String[] columns = line.split(",");

        String installationId = columns[2];

        // TODO codez la mise à jour de l'installation pour ajouter ses équipements

        // Create the first fields
        BasicDBObject equipementObject = new BasicDBObject();
        equipementObject.put("numero", columns[4]);
        equipementObject.put("nom",columns[5]);
        equipementObject.put("type",columns[7]);
        equipementObject.put("famille", columns[9]);
        // Activités
        ActivitesImporter tableauActivites[] = {};
        equipementObject.put("activites", tableauActivites);

        // Updates the installation
        BasicDBObject updateDocument = new BasicDBObject();
        updateDocument.put("$push", new BasicDBObject("equipements", equipementObject));
        BasicDBObject searchQuery = new BasicDBObject().append("_id", installationId);
        installationsCollection.update(searchQuery, updateDocument, true, true);
    }
}
