package nosql.workshop.services;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.CountByActivity;
import org.jongo.Distinct;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Service permettant de manipuler les installations sportives.
 */
public class InstallationService {

    /**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    /**
     * Retourne une installation étant donné son numéro.
     *
     * @param numero le numéro de l'installation.
     * @return l'installation correspondante, ou <code>null</code> si non trouvée.
     */
    public Installation get(String numero) {
        // TODO - DONE - codez le service
        return installations.findOne("{_id: " + numero + "}").as(Installation.class);
    }

    /**
     * Retourne la liste des installations.
     *
     * @param page     la page à retourner.
     * @param pageSize le nombre d'installations par page.
     * @return la liste des installations.
     */
    public List<Installation> list(int page, int pageSize) {
        // TODO - DONE - codez le service
        List<Installation> listInstallation = new ArrayList<Installation>();
        for (Installation i : installations.find().skip((page - 1) * pageSize).limit(pageSize).as(Installation.class)) {
            listInstallation.add(i);
        }
        return listInstallation;
    }

    /**
     * Retourne une installation aléatoirement.
     *
     * @return une installation.
     */
    public Installation random() {
        long count = count();
        int random = new Random().nextInt((int) count);
        // TODO - DONE - codez le service
        Installation installation = null;
        return installations.find().skip(random).limit(1).as(Installation.class).next();
    }

    /**
     * Retourne le nombre total d'installations.
     *
     * @return le nombre total d'installations
     */
    public long count() {
        return installations.count();
    }

    /**
     * Retourne l'installation avec le plus d'équipements.
     *
     * @return l'installation avec le plus d'équipements.
     */
    public Installation installationWithMaxEquipments() {
        // TODO codez le service
        return installations.aggregate("{$project: {nbEquipements : { $size: \"$equipements\"},nom: 1,equipements: 1}}")
                .and("{$sort:{\"nbEquipements\" : -1}}")
                .and("{$limit : 1}")
                .as(Installation.class).get(0);
    }

    /**
     * Compte le nombre d'installations par activité.
     *
     * @return le nombre d'installations par activité.
     */
    public List<CountByActivity> countByActivity() {
        // TODO codez le service

        List<CountByActivity> listCountByActivities = new ArrayList<CountByActivity>();
/*
        Distinct test = installations.distinct("equipements.activites");

        test.
            CountByActivity countByActivity = new CountByActivity();
            countByActivity.setActivite(s);
        }
*/
        return listCountByActivities;
    }

    public double averageEquipmentsPerInstallation() {
        // TODO codez le service
        throw new UnsupportedOperationException();
    }

    /**
     * Recherche des installations sportives.
     *
     * @param searchQuery la requête de recherche.
     * @return les résultats correspondant à la requête.
     */
    public List<Installation> search(String searchQuery) {
        // TODO codez le service
        throw new UnsupportedOperationException();
    }

    /**
     * Recherche des installations sportives par proximité géographique.
     *
     * @param lat      latitude du point de départ.
     * @param lng      longitude du point de départ.
     * @param distance rayon de recherche.
     * @return les installations dans la zone géographique demandée.
     */
    public List<Installation> geosearch(double lat, double lng, double distance) {
        // TODO codez le service
        throw new UnsupportedOperationException();
    }
}
