package services

import (
	"fmt"

	"github.com/renatospaka/neoflix/pkg/fixtures"
	"github.com/renatospaka/neoflix/pkg/ioutils"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/renatospaka/neoflix/pkg/routes/paging"
)

type FavoriteService interface {
	Save(userId, movieId string) (Movie, error)

	FindAllByUserId(userId string, page *paging.Paging) ([]Movie, error)

	Delete(userId, movieId string) (Movie, error)
}

type neo4jFavoriteService struct {
	loader *fixtures.FixtureLoader
	driver neo4j.Driver
}

func NewFavoriteService(loader *fixtures.FixtureLoader, driver neo4j.Driver) FavoriteService {
	return &neo4jFavoriteService{loader: loader, driver: driver}
}

// Save should create a `:HAS_FAVORITE` relationship between
// the User and Movie ID nodes provided.
//
// If either the user or movie cannot be found, a `NotFoundError` should be thrown.
func (fs *neo4jFavoriteService) Save(userId, movieId string) (_ Movie, err error) {
	session := fs.driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}) 
	defer func() {
		err = ioutils.DeferredClose(session, err)
	}()

	movie, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run(`
				MATCH (u:User {userId: $userId})
				MATCH (m:Movie {tmdbId: $movieId})
				MERGE (u)-[r:HAS_FAVORITE]->(m)
						ON CREATE SET u.createdAt = datetime()
				RETURN m {
					.*,
					favorite: true
				} AS movie`, 
			map[string]interface{}{
				"userId": userId,
				"movieId": movieId,
			},
		)
		if err != nil {
			return nil, err
		}

		record, err := result.Single()
		if err != nil {
			return nil, err
		}

		movie, _ := record.Get("movie")
		return movie.(map[string]interface{}), nil
	})
	if err != nil {
		return nil, err
	}
	session.Close()
	
	return movie.(Movie), nil
}

// FindAllByUserId should retrieve a list of movies that have an incoming :HAS_FAVORITE
// relationship from a User node with the supplied `userId`.
//
// Results should be ordered by the `sort` parameter, and in the direction specified
// in the `order` parameter.
// Results should be limited to the number passed as `limit`.
// The `skip` variable should be used to skip a certain number of rows.
func (fs *neo4jFavoriteService) FindAllByUserId(userId string, page *paging.Paging) (_ []Movie, err error) {
	session := fs.driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead}) 
	defer func() {
		err = ioutils.DeferredClose(session, err)
	}()

	movies, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run(fmt.Sprintf(`
				MATCH (u:User {userId: $userId})-[r:HAS_FAVORITE]->(m:Movie)
				RETURN m {
						.*,
						favorite: true
				} AS movie
				ORDER BY m.`+"`%s`"+` %s
				SKIP $skip
				LIMIT $limit`, 
			page.Sort(),
			page.Order()),
			map[string]interface{}{
				"userId": userId,
				"skip":   page.Skip(),
				"limit":  page.Limit(),
			},
		)
		if err != nil {
			return nil, err
		}

		recoords, err := result.Collect()
		if err != nil {
			return nil, err
		}

		var movies []map[string]interface{}
		for _, record := range recoords {
			movie, _ := record.Get("movie")
			movies = append(movies, movie.(map[string]interface{}))
		}
		return movies, nil
	})
	session.Close()

	return movies.([]Movie), nil
}

// Delete should remove the `:HAS_FAVORITE` relationship between
// the User and Movie ID nodes provided.
// If either the user, movie or the relationship between them cannot be found,
// a `NotFoundError` should be thrown.
func (fs *neo4jFavoriteService) Delete(userId, movieId string) (_ Movie, err error) {
	session := fs.driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}) 
	defer func() {
		err = ioutils.DeferredClose(session, err)
	}()

	movie, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run(`
				MATCH (u:User {userId: $userId})-[r:HAS_FAVORITE]->(m:Movie {tmdbId: $movieId})
				DELETE r
				RETURN m {
					.*,
					favorite: false
				} AS movie`, 
			map[string]interface{}{
				"userId": userId,
				"movieId": movieId,
			},
		)
		if err != nil {
			return nil, err
		}

		record, err := result.Single()
		if err != nil {
			return nil, err
		}

		movie, _ := record.Get("movie")
		return movie.(map[string]interface{}), nil
	})
	session.Close()
	
	return movie.(Movie), nil
}
