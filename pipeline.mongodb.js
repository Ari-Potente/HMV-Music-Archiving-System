const fs = require('fs');
fs.writeFileSync("./mongo.log", "")
logToFile = function(data) {
    fs.appendFileSync("./mongo.log", data + '\n', function(err) {
        if(err) {
            return console.log(err);
        }

        console.log("Logfile saved");
    }); 
}


logToFile("-----STARTING NEW MIGRATION AT " + Date().toString() + "-----" + "\n\n")

use('Prueba')
logToFile("Using Prueba as database")

db.persons.drop()
db.createCollection('persons')

db.artists.drop()
db.createCollection('artists')
db.getCollection('artists')
  .createIndex(
    {
        "artistic_name.name": 1,
        "artistic_name.country": 1,
    }, 
    {
        unique: true
    }
  );
  db.getCollection('artists')
  .createIndex(
    {
        "artistic_name.name": 1,
    }, 
    {
        unique: true
    }
  );

db.compositions.drop()
db.createCollection('compositions')
db.getCollection('compositions')
  .createIndex(
    {
      "name": 1,
      "titulo": 1
    }, 
    {
        unique: true
    }
  );

db.concerts.drop()
db.createCollection('concerts')
db.getCollection('concerts')
  .createIndex(
    {
      "city": 1,
      "country": 1,
      "duration": 1,
      "start_date": 1
    }, 
    {
        unique: true
    }
  );


logToFile("Finished dropping the collections: persons, artists, concerts ✅")

// Solistas que no matchean (11 documentos)
db.getCollection('interpretes')
.updateMany(
	{
		$expr: { $and: [ { $eq:  ["$rol", "Solist"]  }, { $ne: ["$interprete_o_banda", "$miembro"]  } ] },
	},
	{ 
		$set: { interprete_o_banda : "$miembro"}
	}
);

logToFile("Started inserting from temas to persons.")
start = Date.now()
db.temas.aggregate([
	{
		$group: {
			_id: "$pasaporte_autor",
			complete_name: {
				$first: "$autor"
			}
		}
	},
	{
		$set: {
			country: { 
				$substrBytes: [ "$_id", 0, 2 ] 
			}
		}
	},
	{
		$merge: {
		  into: 'persons',
		  whenMatched: 'merge',
		  whenNotMatched: 'insert'
		}
	}
]);
logToFile("Finished inserting from temas to persons. Took " + ( Date.now()-start)/1000 + " seconds. ✅")


logToFile("Started inserting from interpretes to persons.")
start = Date.now()
db.interpretes.aggregate([
	{
		$group: {
		  _id: "$pasaporte",
		  complete_name: {
			$first: "$miembro"
		  },
		  birth_date: {
			$first: "$fecha_nacimiento"
		  }
		}
	},
	{
		$set: {
			country: { 
				$substrBytes: [ "$_id", 0, 2 ] 
			},
			birth_date: {
				$dateFromString: {
					"dateString": "$birth_date",
					"format": "%d/%m/%Y",
					"onError": "$$REMOVE",
					"onNull": "$$REMOVE"
				}
			},
		}
	},
	{
		$merge: {
		  into: 'persons',
		  whenMatched: 'merge',
		  whenNotMatched: 'insert'
		}
	}
]);
logToFile("Finished inserting from interpretes to persons. Took " + ( Date.now()-start)/1000 + " seconds. ✅")


logToFile("Started inserting from conciertos to persons.")
start = Date.now()
db.conciertos.aggregate([
	{
		$group: {
		  _id: "$pasaporte_autor",
		  complete_name: {
			$first: "$autor"
		  }
		}
	},
	{
		$set: {
			country: { 
				$substrBytes: [ "$_id", 0, 2 ] 
			}
		}
	},
	{
		$merge: {
		  into: 'persons',
		  whenMatched: 'merge',
		  whenNotMatched: 'insert'
		}
	}
]);
logToFile("Finished inserting from conciertos to persons. Took " + (Date.now()-start)/1000 + " seconds. ✅")

// Add to artists
logToFile("Started inserting from interpretes to artists (only Solists).")
start = Date.now()

db.interpretes.aggregate([
	{
	  $match: {
		$expr: {
		  $eq: [
			"$interprete_o_banda",
			"$miembro"
		  ]
		}
	  }
	},
	{
	  $project: {
		start_date: {
			$dateFromString: {
				"dateString": "$incorporacion",
				"format": "%d/%m/%Y",
				"onError": "$$REMOVE",
				"onNull": "$$REMOVE"
			}
		},
		end_date: {
			$dateFromString: {
				"dateString": "$cese",
				"format": "%d/%m/%Y",
				"onError": "$$REMOVE",
				"onNull": "$$REMOVE"
			}
		},
		artistic_name: {
			name: "$interprete_o_banda",
			country: "$nacionalidad_registro"
		},
		musician: {
		  personId: "$pasaporte"
		},
		_id: 0
	  }
	},
    // Get all roles that a Solist has participated throught their carreer
    {
        $lookup: {
          from: "interpretes",
          localField: "musician.personId",
          foreignField: "pasaporte",
          pipeline: [
            {
                $project: {"_id": 0, "rol": 1}
            }
          ],
          as: "musician.roles"
        }
    },
    // Transform from array of objects to array of strings
    {
        $set: {
            "musician.roles": {
                $reduce: {
                    "input": "$musician.roles",
                    "initialValue": [],
                    "in": {
                        $concatArrays: [ "$$value", ["$$this.rol"]]
                    }
                }
            }
        }
    },
	{
      $lookup: {
        from: "temas",
        localField: "musician.personId",
        foreignField: "pasaporte_autor",
        as: "musician.isWriter"
      }
    },
    {
        $set: {
            "musician.roles": {

                    $cond: [
                        {
                            $gt: [
                                {
                                    $size: "$musician.isWriter"
                                },
                                0
                            ]
                        },
                        {
                            $concatArrays: [
                                "$musician.roles", ["Writer"]
                            ]
                        },
                        "$musician.roles"
                    ]

            }
        }
    },
    {
        $unset: [
            "musician.isWriter"
        ]
    },
	{
	  $merge: {
		into: "artists",
		on: ["artistic_name.name", "artistic_name.country"],
		whenMatched: "merge",
		whenNotMatched: "insert"
	  }
	}
]);
logToFile("Finished inserting from interpretes to artists (only Solists). Took " + ( Date.now()-start)/1000 + " seconds. ✅")

logToFile("Started inserting from temas to artists (only Writers).")
start = Date.now()

db.temas.aggregate([
	{
	    $set: {
            musician: {
                personId: "$pasaporte_autor",
                roles: [ "Writer" ]
            },
            artistic_name: {
                name: "$autor",
            }
        }
	},
    {
        $unset: ["_id", "titulo", "pasaporte_autor", "autor"]
    },
	{
	  $merge: {
		into: "artists",
		on: "artistic_name.name",
		whenMatched: "merge",
		whenNotMatched: "insert"
	  }
	}
]);
logToFile("Finished inserting from temas to artists (only Writers). Took " + ( Date.now()-start)/1000 + " seconds. ✅")


logToFile("Started inserting from interpretes to artists (only Bands).")
start = Date.now()

db.interpretes.aggregate([
	{
	  $match: {
		$expr: {
            $and: [
              {
                $ne: [
                  "$interprete_o_banda",
                  "$miembro"
                  ]
              },
              {
                $ne: [
                  "$rol",
                  "Solist"
                  ]
              },
            ]        
		}
	  }
	},
    {
        $group: {
          _id: "$interprete_o_banda",
          nacionalidad_registro: {$first: "$nacionalidad_registro"},
          members: {
            $addToSet: {
                pasaporte: "$pasaporte",
                start_date: {
                    $cond: [
                        { 
                            $eq: [
                                { $strLenCP: "$incorporacion" },
                                10
                            ]
                        },
                        {
                            $dateFromString: {
                                "dateString": "$incorporacion",
                                "format": "%d/%m/%Y"
                            }
                        },
                        { 
                            $literal: null
                        }
                    ]
                },
                end_date: {
                    $cond: [
                        { 
                            $eq: [
                                { $strLenCP: "$cese" },
                                10
                            ]
                        },
                        {
                            $dateFromString: {
                                "dateString": "$cese",
                                "format": "%d/%m/%Y"
                            }
                        },
                        { 
                            $literal: null
                        }
                    ]
                },
                roles: "$rol"
                }
            }
        }
    },
    {
        $set: {
            type_grouping: "Band",
            artistic_name: {
                name: "$_id",
                country: "$nacionalidad_registro"
            }
        }
    },
    {
        $unset: ["_id", "nacionalidad_registro"]
    },
	{
	  $merge: {
		into: "artists",
		on: ["artistic_name.name", "artistic_name.country"],
		whenMatched: "merge",
		whenNotMatched: "insert"
	  }
	}
]);

logToFile("Finished inserting from interpretes to artists (only Bands). Took " + (Date.now()-start)/1000 + " seconds. ✅")

logToFile("Started inserting from temas to compositions.")
start = Date.now()
// temas to compositions
db.temas.aggregate([
	{
		$set: {
			"name": "$autor",
            "createdBy": "$titulo"
		}
	},
	{
		$unset: [
			"_id"
		]
	},
    {
        $lookup: {
          from: "artists",
          localField: "pasaporte_autor",
          foreignField: "musician.personId",
          pipeline: [
            {
                $project: {"_id": 1 }
            }
          ],
          as: "createdById"
        }
    },
    {
        $set: {
            "createdBy": {
                $first: "$createdById._id"
            },
            "pasaporte_titulo": {
              $concat: ["$pasaporte_autor", "$titulo"]
            }
        }
    },
    {
        $unset: [
            "createdById",
            "autor"
        ]
    },
    {
      $merge: {
        into: "compositions",
        on: ["titulo", "name"],
        whenMatched: "merge",
        whenNotMatched: "insert"
      }
    }
]);
logToFile("Finished inserting from temas to compositions. Took " + (Date.now()-start)/1000 + " seconds. ✅")




logToFile("Started inserting from conciertos to concerts.")
start = Date.now()

db.conciertos.aggregate([
  {
    $limit: 100000000
  },
  {
      $lookup: {
          from: "artists",
          localField: "interprete",
          foreignField: "artistic_name.name",
          pipeline: [
              {
                  $project: {
                      "_id": 1,
                  }
              }
          ],
          as: "result"
      },
  },
  {
      $set: {
          "interprete": {
              $first: "$result._id"
          },
          "pasaporte_tema": {
              $concat: [
                  "$pasaporte_autor",
                  "$tema"
              ]
          }
      }
  },
  {
      $group: {
        _id: {
          artistId: "$interprete",
          tour: "$gira",
          start_date: "$fecha",
          city: "$lugar",
          country: "$pais",
          duration: "$duration"
        },
        compositions: {
          $push: "$pasaporte_tema"
        },
        duration: {
          $first: "$duracion_total"
        }
      }
  },
  {
      $addFields: {
          type_interpretation: "Principal"
      }
  },
  {
      $lookup: {
        from: "compositions",
        localField: "compositions",
        foreignField: "pasaporte_titulo",
        pipeline: [
          {
              $project: {
                "compositionId": "$_id",
                "_id": 0
              }
          }
        ],
        as: "compositions"
      }
  },
  {
      $group: {
          _id: {
              country: "$_id.country",
              city: "$_id.city",
              duration: "$duration",
              start_date: "$_id.start_date",
          },
          artists_played: {
              $push: {
                  artistId: "$_id.artistId",
                  tour: "$_id.tour",
                  type_interpretation: "$type_interpretation",
                  compositions: "$compositions"
              }
          }
      }
  },
  {
    $set: {
      start_date: {
          $dateFromString: {
          "dateString": "$_id.start_date",
          "format": "%d/%m/%Y",
          "onError": "$_id.start_date",
          "onNull": "$$REMOVE"
      }
      },
      city: "$_id.city",
      country: "$_id.country",
      duration: {
          $toInt: "$_id.duration"
      }
    }
  },
  {
      $unset: "_id"
  },
  {
    $out: "concerts"
  }
],
{
  allowDiskUse: true,
  maxTimeMS: 50400000 // 12h
});

logToFile("Finished inserting from conciertos to concerts. Took " + (Date.now()-start)/1000 + " seconds. ✅")

logToFile("Started cleaning from compositions.")
start = Date.now()
db.compositions.aggregate([
  {
    $unset: [
      "pasaporte_autor",
      "pasaporte_titulo"
    ]
  },
  {
    $out: "compositions"
  }
]);
logToFile("Finished cleaning from compositions. Took " + (Date.now()-start)/1000 + " seconds. ✅")

logToFile("Started creating recordings.")
start = Date.now()
db.compositions.aggregate([
  {
    $unset: '_id' // Generates new Id
  },
  {
    $out: 'recordings'
  }
])

logToFile("Finished creating recordings. Took " + (Date.now()-start)/1000 + " seconds. ✅")