package Elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/olivere/elastic/v7"
	"github.com/sha1sum/aws_signing_client"
	"net/http"
	"reflect"
)

type ElasticClient struct {
	AwsClient     *http.Client
	ElasticObject *elastic.Client
}

func GetElasticClient(implementation string, URL string) (*ElasticClient, error) {
	toReturn := new(ElasticClient)

	if implementation == "lambda" {
		credentials := session.New().Config.Credentials
		signer := v4.NewSigner(credentials)

		awsClient, err := aws_signing_client.New(signer, nil, "es", "ap-southeast-2")
		if err != nil {
			return nil, err
		}

		toReturn.AwsClient = awsClient
		elasticClient, elasticError := elastic.NewClient(
			elastic.SetURL(URL),
			elastic.SetScheme("https"),
			elastic.SetHttpClient(awsClient),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(false),
		)

		if elasticError != nil {
			return nil, elasticError
		}

		resp, awsGetErr := awsClient.Get(URL)

		if awsGetErr != nil {
			return nil, awsGetErr
		}

		if resp.StatusCode != 200 {
			return nil, errors.New("Error signing requests")
		}

		toReturn.ElasticObject = elasticClient
	} else {
		//credentials.NewStaticCredentials("IAM_USER_ID", "IAM_USER_SECRET", "")
		return nil, errors.New("Unknown implementation")
	}

	return toReturn, nil
}

func (g *ElasticClient) SafeIndex(id string, document interface{}, index string) (string, error) {
	ctx := context.Background()

	exists, err := g.ElasticObject.IndexExists(index).Do(ctx)
	if err != nil {
		return "", err
	}
	if !exists {
		fmt.Println("Creating new index as none existed for ", index)
		// Create a new index.
		createIndex, err := g.ElasticObject.CreateIndex(index).Do(ctx)
		if err != nil {
			return "", err
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
			return "", errors.New("Create index not acknowledged")
		}
	}

	marshalled, marshalErr := json.Marshal(document)

	if marshalErr != nil {
		return "", marshalErr
	}

	stringMarshal := string(marshalled)
	fmt.Println(stringMarshal)

	// Index a tweet (using JSON serialization)
	put1, err := g.ElasticObject.Index().
		Index(index).
		Id(id).
		BodyString(stringMarshal).
		Do(ctx)
	if err != nil {
		return "", err
	}

	return put1.Id, nil
}

func (g *ElasticClient) DeleteByID(id string, index string) error {
	ctx := context.Background()
	_, err := g.ElasticObject.Delete().
		Index(index).
		Id(id).
		Do(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (g *ElasticClient) GetByID(id string, marshalTo interface{}, index string) (interface{}, error) {
	ctx := context.Background()
	elasticValue, elasticErr := g.ElasticObject.Get().Index(index).Id(id).Do(ctx)
	if elasticErr != nil {
		return nil, elasticErr
	}

	marshalErr := json.Unmarshal([]byte(elasticValue.Source), marshalTo)
	if marshalErr != nil {
		return nil, marshalErr
	}

	return marshalTo, nil
}

func (g *ElasticClient) Search(index string, field string, searchTerm string, marshalTo interface{}) ([]interface{}, error) {
	matchQuery := elastic.NewMatchPhrasePrefixQuery(field, searchTerm)
	searchResult, err := g.ElasticObject.Search().
		Index(index).
		Query(matchQuery).
		From(0).
		Size(10).Do(context.Background())

	if err != nil {
		return nil, err
	}

	return searchResult.Each(reflect.TypeOf(marshalTo)), nil
}

func (g *ElasticClient) SearchByLocation(index string, field string, distance string, lat float64, lon float64, marshalTo interface{}) ([]interface{}, error) {
	locQuery := elastic.NewGeoDistanceQuery(field).Lat(lat).Lon(lon).Distance(distance)

	searchResult, err := g.ElasticObject.Search().
		Index(index).
		Query(locQuery).
		From(0).
		Size(10).Do(context.Background())

	if err != nil {
		return nil, err
	}

	return searchResult.Each(reflect.TypeOf(marshalTo)), nil
}
