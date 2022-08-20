package services

import (
	"github.com/renatospaka/neoflix/pkg/fixtures"
	"github.com/renatospaka/neoflix/pkg/ioutils"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/renatospaka/neoflix/pkg/routes/paging"
)

type Rating = map[string]interface{}

type RatingService interface {
	FindAllByMovieId(id string, page *paging.Paging) ([]Rating, error)

	Save(rating int, movieId string, userId string) (Movie, error)
}

type neo4jRatingService struct {
	loader *fixtures.FixtureLoader
	driver neo4j.Driver
}

func NewRatingService(loader *fixtures.FixtureLoader, driver neo4j.Driver) RatingService {
	return &neo4jRatingService{loader: loader, driver: driver}
}

// FindAllByMovieId returns a paginated list of reviews for a Movie.
//
// Results should be ordered by the `sort` parameter, and in the direction specified
// in the `order` parameter.
// Results should be limited to the number passed as `limit`.
// The `skip` variable should be used to skip a certain number of rows.
func (rs *neo4jRatingService) FindAllByMovieId(movieId string, page *paging.Paging) (_ []Rating, err error) {
	return rs.loader.ReadArray("fixtures/ratings.json")
}

// Save adds a relationship between a User and Movie with a `rating` property.
// The `rating` parameter should be converted to a Neo4j Integer.
//
// If the User or Movie cannot be found, a NotFoundError should be thrown
func (rs *neo4jRatingService) Save(rating int, movieId string, userId string) (_ Movie, err error) {
	session := rs.driver.NewSession(neo4j.SessionConfig{})
	defer func() {
		err = ioutils.DeferredClose(session, err)
	}()

	result, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run(`
			MATCH (u:User {userId: $userId})
			MATCH (m:Movie {tmdbId: $movieId})

			MERGE (u)-[r:RATED]->(m)
			SET r.rating = $rating, r.timestamp = timestamp()

			RETURN m { .*, rating: r.rating } AS movie`, 
			map[string]interface{}{
				"userId": userId,
				"movieId": movieId,
				"rating": rating,
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

	return result.(Movie), nil
}
