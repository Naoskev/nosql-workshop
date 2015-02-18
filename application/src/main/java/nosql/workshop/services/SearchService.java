package nosql.workshop.services;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.index.query.QueryBuilders.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 *
 * Created by Chris on 12/02/15.
 */
public class SearchService {
    public static final String INSTALLATIONS_INDEX = "installations";
    public static final String INSTALLATION_TYPE = "installation";
    public static final String TOWNS_INDEX = "towns";
    private static final String TOWN_TYPE = "town";


    public static final String ES_HOST = "es.host";
    public static final String ES_TRANSPORT_PORT = "es.transport.port";

    final Client elasticSearchClient;
    final ObjectMapper objectMapper;

    @Inject
    public SearchService(@Named(ES_HOST) String host, @Named(ES_TRANSPORT_PORT) int transportPort) {

        elasticSearchClient = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name","Filoutubs").build())
        .addTransportAddress(new InetSocketTransportAddress(host, transportPort));

        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Recherche les installations à l'aide d'une requête full-text
     * @param searchQuery la requête
     * @return la listes de installations
     */
    public List<Installation> search(String searchQuery) {
        // TODO - DONE - codez le service
        SearchResponse response = elasticSearchClient.prepareSearch(INSTALLATIONS_INDEX)
                .setTypes(INSTALLATION_TYPE)
                .setQuery(QueryBuilders.queryString(searchQuery))
                .execute()
                .actionGet();

        List<Installation> installationList = new ArrayList<Installation>();
        for (SearchHit sh : response.getHits()) {
            installationList.add(mapToInstallation(sh));
        }
        return installationList;
    }

    /**
     * Transforme un résultat de recherche ES en objet installation.
     *
     * @param searchHit l'objet ES.
     * @return l'installation.
     */
    private Installation mapToInstallation(SearchHit searchHit) {
        try {
            return objectMapper.readValue(searchHit.getSourceAsString(), Installation.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Transforme un résultat de recherche ES en objet townSuggest.
     *
     * @param searchHit l'objet ES.
     * @return townSuggest.
     */
    private TownSuggest mapToTownSuggest(SearchHit searchHit) {
        try {
            return objectMapper.readValue(searchHit.getSourceAsString(), TownSuggest.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<TownSuggest> suggestTownName(String townName){
        // TODO - DONE - codez le service
        SearchResponse response = elasticSearchClient.prepareSearch(TOWNS_INDEX)
                .setTypes(TOWN_TYPE)
                .setQuery(QueryBuilders.wildcardQuery("townName", townName + "*"))
                .execute()
                .actionGet();

        List<TownSuggest> townSuggestList = new ArrayList<TownSuggest>();
        for (SearchHit sh : response.getHits()) {
            townSuggestList.add(mapToTownSuggest(sh));
        }
        return townSuggestList;
    }

    public Double[] getTownLocation(String townName) {
        // TODO codez le service
        QueryBuilder query = QueryBuilders.matchQuery("townName", townName);
        Double [] location = null;

        try {
            SearchResponse response = this.elasticSearchClient.prepareSearch("towns","town").setQuery(query).execute().get();
            if(response.getHits().totalHits() > 0){

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return location;
    }
}
