package main

import (
	"Archival_Tool/mongoutil"
	"Archival_Tool/utils"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var configDoc Config

const ARCHIVAL_TOOL_CONFIG = "./archival_tool.json"

type Collection struct {
	DbName     string                 `json:"db"`
	ColName    string                 `json:"collection"`
	Date       string                 `json:"Date"`
	Attributes map[string]interface{} `json:"attributes"`
}
type Config struct {
	Global     []Collection `json:"global"`
	Enterprise []Collection `json:"enterprise"`
}

var sessions []string

func initConfig() Config {
	jsonFile, err := os.Open("archival_tool.json")
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()
	var result Config
	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal([]byte(byteValue), &result)

	return result
}

func getPrefixColl(colList []string, prefix string) []string {
	var dbcol []string
	for _, j := range colList {
		if strings.HasPrefix(j, prefix) {
			dbcol = append(dbcol, j)
		}
	}
	return dbcol
}

func ArchiveCollections() {
	fmt.Println("Archiving Tool Started")
	// client, ctx, cancel, err := mongoutil.Connect("mongodb://localhost:27017")
	client, ctx, cancel, err := mongoutil.Connect("mongodb://ushurUser:ushur@localhost:57017/sms")
	client1, ctx1, cancel1, err1 := mongoutil.Connect("mongodb://ushurUser:ushur@localhost:57017/mobilyze")

	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	if err1 != nil {
		fmt.Println(err1)
		panic(err1)
	}
	defer mongoutil.Close(client, ctx, cancel)
	defer mongoutil.Close(client1, ctx1, cancel1)

	err = mongoutil.Ping(client, ctx)
	if err != nil {
		fmt.Println(err)
	}
	// else {
	// 	fmt.Println("Ping Success")
	// }

	fmt.Println("----------------------------------Global Collection Archiving Started---------------------------")
	ArchiveGlobalCollections(ctx, client, configDoc.Global)
	fmt.Println("----------------------------------Enterprise Collection Archiving Started---------------------------")
	ArchiveEnterpriseCollection(ctx, client, ctx1, client1, configDoc.Enterprise, cancel)
}
func ArchiveGlobalCollections(ctx context.Context, client *mongo.Client, coll []Collection) {
	fmt.Printf("%-30s %-15s %-15s\n", " ", "Database", "Collection")

	for _, v := range coll {
		fmt.Printf("\n%-30s %-15s %-15s\n", ">>>>", v.DbName, v.ColName)

		baseCollection := client.Database(v.DbName).Collection(v.ColName)

		initial_count, err := baseCollection.CountDocuments(ctx, bson.D{})
		if err != nil {
			fmt.Println("InitialCount ERROR:", err)
		}

		currentTime := time.Now().AddDate(0, 0, -2).UTC()
		bytes, err := currentTime.MarshalJSON()
		if err != nil {
			fmt.Println("Marshal Json Error :", err)
		}
		fmt.Println(".......... Archiving Started ..........")
		_, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + v.ColName + ` -q '{ "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }' -u ushurUser -p ushur --port=57017 `)
		// fmt.Println("mongodump --db=" + v.DbName + " --collection=" + v.ColName + ` -q '{ "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }' `)
		if err != nil {
			fmt.Println("mongodump ERROR:", err, stderrout)
		}
		timestamp := utils.CompressedTimeFormat(currentTime)
		_, _, err1 := utils.Shellout("mongorestore --db=" + v.DbName + " --collection=" + "__" + v.ColName + "_" + timestamp + " ./dump/" + v.DbName + "/" + v.ColName + ".bson -u ushurUser -p ushur --port=57017")
		// _, _, err1 := utils.Shellout("mongorestore --db=" + v.DbName + " --collection=" + v.ColName + "_" + timestamp + " ./dump/" + v.DbName + "/" + v.ColName + ".bson" + " -u ushurUser -p ushur --port=57017")

		if err1 != nil {
			fmt.Println("mongorestore ERROR: ", err1)
		}
		fmt.Println(".......... Archiving Finished ..........")

		newDocumentCount, err := client.Database(v.DbName).Collection("__"+v.ColName+"_"+timestamp).CountDocuments(ctx, bson.D{})
		if err != nil {
			fmt.Println("ArchivedCollectionCount ERROR", err)
		}
		_, er := baseCollection.DeleteMany(ctx, bson.M{v.Date: bson.M{
			"$lt": currentTime,
		}})
		if er != nil {
			fmt.Println("baseCollectionDelete: ", er)
		}
		baseDocumentCount, err := baseCollection.CountDocuments(ctx, bson.D{})
		if err != nil {
			fmt.Println("baseDocumentCount ERROR:", err)
		}
		fmt.Printf("%-30s %-15d\n", "INITIAL DOCUMENT COUNT : ", initial_count)
		fmt.Printf("%-30s %-15d\n", "ARCHIVED DOCUMENT COUNT : ", newDocumentCount)
		fmt.Printf("%-30s %-15d\n", "FINAL_DOCUMENT_COUNT : ", baseDocumentCount)

		if baseDocumentCount+newDocumentCount == initial_count {
			fmt.Println("Archived Count(", newDocumentCount, ") + Final Count(", baseDocumentCount, ") = Initial Count(", initial_count, ") => Data is Consistent")
		}
	}
}
func ArchiveEnterpriseCollection(ctx context.Context, client *mongo.Client, ctx1 context.Context, client1 *mongo.Client, dbs []Collection, cancel context.CancelFunc) {
	fmt.Printf("%-30s %-15s %-15s\n", " ", "Database", "Collection")

	for _, v := range dbs {

		// if v.DbName == "mobilyze" {
		// 	mongoutil.Close(client, ctx, cancel)
		// 	client, ctx, _, _ = mongoutil.Connect("mongodb://ushurUser:ushur@localhost:57017/mobilyze")
		// 	// if err != nil {
		// 	// 	fmt.Println(err)
		// 	// 	panic(err)
		// 	// }
		// }
		if v.DbName == "mobilyze" {
			allCollectionInDb, _ := client1.Database(v.DbName).ListCollectionNames(ctx, bson.M{})
			// fmt.Println(allCollectionInDb)
			allCollectionInDb = getPrefixColl(allCollectionInDb, v.ColName)
			// fmt.Println(allCollectionInDb)

			for _, entColName := range allCollectionInDb {

				fmt.Printf("\n%-30s %-15s %-15s\n", ">>>>", v.DbName, entColName)
				baseCollection := client1.Database(v.DbName).Collection(entColName)

				initial_count, err := baseCollection.CountDocuments(ctx1, bson.D{})

				if err != nil {
					fmt.Println("InitialCount :", err)
				}
				var isNotSentCount int64 = 0
				currentTime := time.Now().AddDate(0, -1, 0).UTC()
				bytes, err := currentTime.MarshalJSON()
				if err != nil {
					fmt.Println("Marshal Json Error :", err)
				}
				fmt.Println(".......... Archiving Started ..........")
				if v.ColName == "NotificationRecords" {

					isNotSentCount, err = baseCollection.CountDocuments(ctx1, bson.M{"updatedDate": bson.M{
						"$lt": currentTime,
					}, "isNotSent": true})
					fmt.Println("NotSentNotificationRecords Count : ", isNotSentCount)
					// _, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName + ` -q '{ "isNotSent" : false , "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }' -u ushurUser -p ushur --port=57017`)
					_, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName + ` -q '{ "isNotSent" : false , "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }'  -u ushurUser -p ushur --port=57017`)

					if err != nil {
						fmt.Println("mongodump ERROR:", err, stderrout)
					}
				} else {
					// _, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName + ` -q '{ "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }' -u ushurUser -p ushur --port=57017`)
					_, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName + ` -q '{ "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }'  -u ushurUser -p ushur --port=57017`)

					if err != nil {
						fmt.Println("mongodump ERROR:", err, stderrout)
					}
				}

				// _, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName  + ` -q '{ "` + v.Attributes["Date"] + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }' `)

				timestamp := utils.CompressedTimeFormat(currentTime)
				_, _, err1 := utils.Shellout("mongorestore --db=" + v.DbName + " --collection=" + "__" + entColName + "_" + timestamp + " ./dump/" + v.DbName + "/" + entColName + ".bson -u ushurUser -p ushur --port=57017")
				// _, _, err1 := utils.Shellout("mongorestore --db=" + v.DbName + " --collection=" + entColName + "_" + timestamp + " ./dump/" + v.DbName + "/" + entColName + ".bson" + " -u ushurUser -p ushur --port=57017")

				if err1 != nil {
					fmt.Println("mongorestore ERROR: ", err1)
				}
				fmt.Println(".......... Archiving Finished ..........")
				newDocumentCount, err := client1.Database(v.DbName).Collection("__"+entColName+"_"+timestamp).CountDocuments(ctx1, bson.D{})
				if err != nil {
					fmt.Println("ArchivedCollectionCount ERROR :", err)
				}
				// if v.ColName == "NotificationRecords" {
				// 	_, er := baseCollection.DeleteMany(ctx, bson.M{v.Date: bson.M{
				// 		"$lt": currentTime,
				// 	}, "isNotSent ": false})
				// 	if er != nil {
				// 		fmt.Println("NotificationRecords Cant delete isNotSent documents ERROR: ", er)
				// 	}
				// }
				_, er := baseCollection.DeleteMany(ctx, bson.M{v.Date: bson.M{
					"$lt": currentTime,
				}})
				if er != nil {
					fmt.Println("baseCollectionDelete ERROR : ", er)
				}
				baseDocumentCount, err := baseCollection.CountDocuments(ctx1, bson.D{})
				if err != nil {
					fmt.Println("baseDocumentCount ERROR:", err)
				}
				fmt.Printf("%-30s %-15d\n", "INITIAL DOCUMENT COUNT : ", initial_count)
				fmt.Printf("%-30s %-15d\n", "ARCHIVED DOCUMENT COUNT : ", newDocumentCount)
				fmt.Printf("%-30s %-15d\n", "FINAL_DOCUMENT_COUNT : ", baseDocumentCount)

				if v.ColName == "NotificationRecords" {
					if baseDocumentCount+newDocumentCount+isNotSentCount == initial_count {
						fmt.Println("Archived Count(", newDocumentCount, ") + NotSentNotificationRecords Count(", newDocumentCount, ") + Final Count(", baseDocumentCount, ") = Initial Count(", initial_count, ") => Data is Consistent")
					}
				} else {

					if baseDocumentCount+newDocumentCount == initial_count {
						fmt.Println("Archived Count(", newDocumentCount, ") + Final Count(", baseDocumentCount, ") = Initial Count(", initial_count, ") => Data is Consistent")
					}
				}
			}
		} else {
			allCollectionInDb, _ := client.Database(v.DbName).ListCollectionNames(ctx, bson.M{})
			// fmt.Println(allCollectionInDb)
			allCollectionInDb = getPrefixColl(allCollectionInDb, v.ColName)
			// fmt.Println(allCollectionInDb)
			for _, entColName := range allCollectionInDb {

				fmt.Printf("\n%-30s %-15s %-15s\n", ">>>>", v.DbName, entColName)
				baseCollection := client.Database(v.DbName).Collection(entColName)

				initial_count, err := baseCollection.CountDocuments(ctx, bson.D{})

				if err != nil {
					fmt.Println("InitialCount :", err)
				}
				var isNotSentCount int64 = 0
				currentTime := time.Now().AddDate(0, -1, 0).UTC()
				bytes, err := currentTime.MarshalJSON()
				if err != nil {
					fmt.Println("Marshal Json Error :", err)
				}
				fmt.Println(".......... Archiving Started ..........")
				if v.ColName == "NotificationRecords" {

					isNotSentCount, err = baseCollection.CountDocuments(ctx, bson.M{"updatedDate": bson.M{
						"$lt": currentTime,
					}, "isNotSent": true})
					fmt.Println("NotSentNotificationRecords Count : ", isNotSentCount)
					// _, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName + ` -q '{ "isNotSent" : false , "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }' -u ushurUser -p ushur --port=57017`)
					_, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName + ` -q '{ "isNotSent" : false , "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }'  -u ushurUser -p ushur --port=57017`)

					if err != nil {
						fmt.Println("mongodump ERROR:", err, stderrout)
					}
				} else {
					// _, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName + ` -q '{ "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }' -u ushurUser -p ushur --port=57017`)
					_, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName + ` -q '{ "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }'  -u ushurUser -p ushur --port=57017`)

					if err != nil {
						fmt.Println("mongodump ERROR:", err, stderrout)
					}
				}

				// _, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName  + ` -q '{ "` + v.Attributes["Date"] + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }' `)

				timestamp := utils.CompressedTimeFormat(currentTime)
				_, _, err1 := utils.Shellout("mongorestore --db=" + v.DbName + " --collection=" + "__" + entColName + "_" + timestamp + " ./dump/" + v.DbName + "/" + entColName + ".bson -u ushurUser -p ushur --port=57017")
				// _, _, err1 := utils.Shellout("mongorestore --db=" + v.DbName + " --collection=" + entColName + "_" + timestamp + " ./dump/" + v.DbName + "/" + entColName + ".bson" + " -u ushurUser -p ushur --port=57017")

				if err1 != nil {
					fmt.Println("mongorestore ERROR: ", err1)
				}
				fmt.Println(".......... Archiving Finished ..........")
				newDocumentCount, err := client.Database(v.DbName).Collection("__"+entColName+"_"+timestamp).CountDocuments(ctx, bson.D{})
				if err != nil {
					fmt.Println("ArchivedCollectionCount ERROR :", err)
				}
				// if v.ColName == "NotificationRecords" {
				// 	_, er := baseCollection.DeleteMany(ctx, bson.M{v.Date: bson.M{
				// 		"$lt": currentTime,
				// 	}, "isNotSent ": false})
				// 	if er != nil {
				// 		fmt.Println("NotificationRecords Cant delete isNotSent documents ERROR: ", er)
				// 	}
				// }
				_, er := baseCollection.DeleteMany(ctx, bson.M{v.Date: bson.M{
					"$lt": currentTime,
				}})
				if er != nil {
					fmt.Println("baseCollectionDelete ERROR : ", er)
				}
				baseDocumentCount, err := baseCollection.CountDocuments(ctx, bson.D{})
				if err != nil {
					fmt.Println("baseDocumentCount ERROR:", err)
				}
				fmt.Printf("%-30s %-15d\n", "INITIAL DOCUMENT COUNT : ", initial_count)
				fmt.Printf("%-30s %-15d\n", "ARCHIVED DOCUMENT COUNT : ", newDocumentCount)
				fmt.Printf("%-30s %-15d\n", "FINAL_DOCUMENT_COUNT : ", baseDocumentCount)

				if v.ColName == "NotificationRecords" {
					if baseDocumentCount+newDocumentCount+isNotSentCount == initial_count {
						fmt.Println("Archived Count(", newDocumentCount, ") + NotSentNotificationRecords Count(", newDocumentCount, ") + Final Count(", baseDocumentCount, ") = Initial Count(", initial_count, ") => Data is Consistent")
					}
				} else {

					if baseDocumentCount+newDocumentCount == initial_count {
						fmt.Println("Archived Count(", newDocumentCount, ") + Final Count(", baseDocumentCount, ") = Initial Count(", initial_count, ") => Data is Consistent")
					}
				}
			}
		}
	}
}
func main() {
	// getAllCollections()
	// initDbColNames()
	configDoc = initConfig()
	ArchiveCollections()
	// ss := getPrefixColl([]string{"suraj", "sura", "eej", "surak", "surjskw"}, "k")
	// fmt.Println(ss)
	// fmt.Printf("%-15s %-15s %-15s", ">>>", "Database", "Collection")
	// if "mobilyze" == "mobilyze" {
	// 	fmt.Print(true)
	// }
}
