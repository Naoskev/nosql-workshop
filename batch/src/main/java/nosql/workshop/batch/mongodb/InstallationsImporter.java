package nosql.workshop.batch.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

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
        // Create the first to field
        BasicDBObject basicDBObject = new BasicDBObject();
        basicDBObject.put("_id",columns[1]);
        basicDBObject.put("nom",columns[0]);
        //Create the address field
        BasicDBObject addressObject = new BasicDBObject();
        addressObject.put("numero",columns[6]);
        addressObject.put("voie",columns[7]);
        addressObject.put("lieuDit",columns[5]);
        addressObject.put("codePostal",columns[4]);
        addressObject.put("commune",columns[2]);
        basicDBObject.put("adresse",addressObject);
        // Create the location field
        BasicDBObject locationObject = new BasicDBObject();
        String coordinates [] = {columns[9],columns[10]};
        locationObject.put("type","Point");
        locationObject.put("coordinates",coordinates);
        basicDBObject.put("location",locationObject);
        // Others fields
        basicDBObject.put("multiCommune", columns[16]);
        basicDBObject.put("nbPlacesParking" ,columns[17]);
        basicDBObject.put("nbPlacesParkingHandicapes", columns[18]);
        // Date field
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateTimeZone timeZone = DateTimeZone.forID( "Europe/Paris" );
        DateTimeFormatter formatter = DateTimeFormat.forPattern("MM-dd-yyyy").withZone( timeZone );
        String input = columns[29];
        DateTime dateTime = formatter.parseDateTime( input );
        String iso8601String = dateTime.toString();
        DateTime dateTime2 = new DateTime( iso8601String, timeZone );
        basicDBObject.put("dateMiseAJourFiche", formatter.print(dateTime2));

        return new BasicDBObject();
    }
}