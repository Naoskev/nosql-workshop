package nosql.workshop.batch.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.*;

/**
 * Importe les 'installations' dans MongoDB.
 */
public class InstallationsImporter {

    private final DBCollection installationsCollection;

    public InstallationsImporter(DBCollection installationsCollection) {
        this.installationsCollection = installationsCollection;
    }

    public void run() {
        InputStream is = CsvToMongoDb.class.getResourceAsStream("/csv/installations.csv");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .forEach(line -> installationsCollection.save(toDbObject(line)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private DBObject toDbObject(final String line) {
        String[] columns = line
                .substring(1, line.length() - 1)
                .split("\",\"");

        // TODO créez le document à partir de la ligne CSV
        //"Nom usuel de l'installation","Numéro de l'installation","Nom de la commune","Code INSEE","Code postal",
        // "Nom du lieu dit","Numero de la voie","Nom de la voie","location","Longitude","Latitude",
        // "Aucun aménagement d'accessibilité","Accessibilité handicapés à mobilité réduite",
        // "Accessibilité handicapés sensoriels","Emprise foncière en m2","Gardiennée avec ou sans logement de gardien",
        // "Multi commune","Nombre total de place de parking","Nombre total de place de parking handicapés",
        // "Installation particulière","Desserte métro","Desserte bus","Desserte Tram","Desserte train","Desserte bateau",
        // "Desserte autre","Nombre total d'équipements sportifs","Nombre total de fiches équipements",
        // "Date de mise à jour de la fiche installation"

        // Create the first to field
        BasicDBObject basicDBObject = new BasicDBObject();
        basicDBObject.put("_id",columns[1]);
        basicDBObject.put("nom",columns[0]);
        //Create the address field
        BasicDBObject addressObject = new BasicDBObject();
        addressObject.put("numero",);

        return new BasicDBObject();
    }
}
