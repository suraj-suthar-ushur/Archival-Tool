package main

import (
	"Archival_Tool/mongoutil"
	"Archival_Tool/utils"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
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
	Username   string       `json:"mongousername"`
	Password   string       `json:"mongopassword"`
	Global     []Collection `json:"global"`
	Enterprise []Collection `json:"enterprise"`
}

type MongoClient struct {
	Client  *mongo.Client
	Context context.Context
}

var dbToContextMap map[string]MongoClient

func initConfig() Config {
	jsonFile, err := os.Open(ARCHIVAL_TOOL_CONFIG)
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

func RemoveDumpFolder(dumpFolder string) {
	os.RemoveAll(dumpFolder)
}
func ArchiveCollections() {
	fmt.Println("Archiving Tool Started")
	utils.Logger().Info("Archiving Tool Started")
	smsClient, smsCtx, smsCancel, err := mongoutil.Connect("mongodb://ushurUser:ushur@localhost:57017/sms")
	mobilyzeClient, mobilyzeCtx, mobilyzeCancel, err1 := mongoutil.Connect("mongodb://ushurUser:ushur@localhost:57017/mobilyze")
	dbToContextMap = make(map[string]MongoClient)
	dbToContextMap["sms"] = MongoClient{Client: smsClient, Context: smsCtx}
	dbToContextMap["mobilyze"] = MongoClient{Client: mobilyzeClient, Context: mobilyzeCtx}

	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	if err1 != nil {
		fmt.Println(err1)
		panic(err1)
	}
	defer mongoutil.Close(smsClient, smsCtx, smsCancel)
	defer mongoutil.Close(mobilyzeClient, mobilyzeCtx, mobilyzeCancel)

	err = mongoutil.Ping(smsClient, smsCtx)
	if err != nil {
		fmt.Println(err)
		utils.Logger().Panic(err)

	}
	err = mongoutil.Ping(mobilyzeClient, mobilyzeCtx)
	if err != nil {
		fmt.Println(err)
		utils.Logger().Panic(err)

	}

	fmt.Println("----------------------------------Global Collection Archiving Started---------------------------")
	ArchiveGlobalCollections(configDoc.Global)
	fmt.Println("----------------------------------Enterprise Collection Archiving Started---------------------------")
	ArchiveEnterpriseCollection(configDoc.Enterprise)
}
func ArchiveGlobalCollections(coll []Collection) {
	fmt.Printf("%-30s %-15s %-15s\n", " ", "Database", "Collection")
	for _, v := range coll {
		fmt.Printf("\n%-30s %-15s %-15s\n", ">>>>", v.DbName, v.ColName)

		baseCollection := dbToContextMap[v.DbName].Client.Database(v.DbName).Collection(v.ColName)

		initial_count, err := baseCollection.CountDocuments(dbToContextMap[v.DbName].Context, bson.D{})
		if err != nil {
			fmt.Println("InitialCount ERROR:", err)
			utils.Logger().Error(err)

		}

		currentTime := time.Now().AddDate(0, 0, 0).UTC()
		bytes, err := currentTime.MarshalJSON()
		if err != nil {
			fmt.Println("Marshal Json Error :", err)
			utils.Logger().Warn(err)

		}
		fmt.Println(".......... Archiving Started ..........")
		utils.Logger().Info("Archiving Started")

		query := ` '{ "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }' `
		credentials := ` -u ` + configDoc.Username + ` -p ` + configDoc.Password + ` --port=57017 `
		_, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + v.ColName + ` -q ` + query + credentials) //give output path here
		if err != nil {
			fmt.Println("mongodump ERROR:", err, stderrout)
			utils.Logger().Panic(err)

		}
		timestamp := utils.CompressedTimeFormat(currentTime)
		_, _, err1 := utils.Shellout("mongorestore --db=" + v.DbName + " --collection=" + "__" + v.ColName + "_" + timestamp + " ./dump/" + v.DbName + "/" + v.ColName + ".bson " + credentials)

		if err1 != nil {
			fmt.Println("mongorestore ERROR: ", err1)
			utils.Logger().Panic(err1)

		}
		fmt.Println(".......... Archiving Finished ..........")
		utils.Logger().Info("Archiving Finished")

		newDocumentCount, err := dbToContextMap[v.DbName].Client.Database(v.DbName).Collection("__"+v.ColName+"_"+timestamp).CountDocuments(dbToContextMap[v.DbName].Context, bson.D{})
		if err != nil {
			fmt.Println("ArchivedCollectionCount ERROR", err)
			utils.Logger().Error(err)

		}
		_, er := baseCollection.DeleteMany(dbToContextMap[v.DbName].Context, bson.M{v.Date: bson.M{
			"$lt": currentTime,
		}})
		if er != nil {
			utils.Logger().Panic(er)
			fmt.Println("baseCollectionDelete: ", er)
		}
		baseDocumentCount, err := baseCollection.CountDocuments(dbToContextMap[v.DbName].Context, bson.D{})
		if err != nil {
			fmt.Println("baseDocumentCount ERROR:", err)
			utils.Logger().Error(err)

		}
		fmt.Printf("%-30s %-15d\n", "INITIAL DOCUMENT COUNT : ", initial_count)
		fmt.Printf("%-30s %-15d\n", "ARCHIVED DOCUMENT COUNT : ", newDocumentCount)
		fmt.Printf("%-30s %-15d\n", "FINAL_DOCUMENT_COUNT : ", baseDocumentCount)

		if baseDocumentCount+newDocumentCount == initial_count {
			fmt.Println("Archived Count(", newDocumentCount, ") + Final Count(", baseDocumentCount, ") = Initial Count(", initial_count, ") => Data is Consistent")
			utils.Logger().Info("Archival Success")
		} else {
			utils.Logger().Panic("Document Count Not Matched")

		}
	}
}
func ArchiveEnterpriseCollection(dbs []Collection) {
	fmt.Printf("%-30s %-15s %-15s\n", " ", "Database", "Collection")

	for _, v := range dbs {

		allCollectionInDb, err := dbToContextMap[v.DbName].Client.Database(v.DbName).ListCollectionNames(dbToContextMap[v.DbName].Context, bson.M{})
		if err != nil {
			fmt.Println(err)
		}
		// fmt.Println(allCollectionInDb)
		allCollectionInDb = getPrefixColl(allCollectionInDb, v.ColName)
		fmt.Println(allCollectionInDb)
		for _, entColName := range allCollectionInDb {

			fmt.Printf("\n%-30s %-15s %-15s\n", ">>>>", v.DbName, entColName)
			baseCollection := dbToContextMap[v.DbName].Client.Database(v.DbName).Collection(entColName)

			initial_count, err := baseCollection.CountDocuments(dbToContextMap[v.DbName].Context, bson.D{})

			if err != nil {
				utils.Logger().Panic(err)

				fmt.Println("InitialCount :", err)
			}
			var isNotSentCount int64 = 0
			currentTime := time.Now().AddDate(0, 0, 0).UTC()
			bytes, err := currentTime.MarshalJSON()
			if err != nil {
				utils.Logger().Error(err)

				fmt.Println("Marshal Json Error :", err)
			}
			fmt.Println(".......... Archiving Started ..........")
			utils.Logger().Info("Archival Started")

			if v.ColName == "NotificationRecords" {

				isNotSentCount, err = baseCollection.CountDocuments(dbToContextMap[v.DbName].Context, bson.M{"updatedDate": bson.M{
					"$lt": currentTime,
				}, "isNotSent": false})
				if err != nil {
					utils.Logger().Error(err)

				}
				fmt.Println("NotSentNotificationRecords Count : ", isNotSentCount)
				query := ` '{ "isNotSent" : true , "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }' `
				credentials := ` -u ` + configDoc.Username + ` -p ` + configDoc.Password + ` --port=57017 `

				_, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName + ` -q  ` + query + credentials)

				if err != nil {
					fmt.Println("mongodump ERROR:", err, stderrout)
					utils.Logger().Panic(err)

				}
			} else {
				query := ` '{ "` + v.Date + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }' `
				credentials := ` -u ` + configDoc.Username + ` -p ` + configDoc.Password + ` --port=57017 `

				_, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName + ` -q ` + query + credentials)

				if err != nil {
					fmt.Println("mongodump ERROR:", err, stderrout)
					utils.Logger().Panic(err)
				}
			}

			// _, stderrout, err := utils.Shellout("mongodump --db=" + v.DbName + " --collection=" + entColName  + ` -q '{ "` + v.Attributes["Date"] + `" : { "$lt" : { "$date": ` + string(bytes) + ` } } }' `)

			timestamp := utils.CompressedTimeFormat(currentTime)
			credentials := ` -u ` + configDoc.Username + ` -p ` + configDoc.Password + ` --port=57017 `

			_, _, err1 := utils.Shellout("mongorestore --db=" + v.DbName + " --collection=" + "__" + entColName + "_" + timestamp + " ./dump/" + v.DbName + "/" + entColName + ".bson " + credentials)
			// _, _, err1 := utils.Shellout("mongorestore --db=" + v.DbName + " --collection=" + entColName + "_" + timestamp + " ./dump/" + v.DbName + "/" + entColName + ".bson" + " -u ushurUser -p ushur --port=57017")

			if err1 != nil {
				fmt.Println("mongorestore ERROR: ", err1)
				utils.Logger().Panic(err)

			}
			fmt.Println(".......... Archiving Finished ..........")
			utils.Logger().Info("Archiving Finished")

			newDocumentCount, err := dbToContextMap[v.DbName].Client.Database(v.DbName).Collection("__"+entColName+"_"+timestamp).CountDocuments(dbToContextMap[v.DbName].Context, bson.D{})
			if err != nil {
				fmt.Println("ArchivedCollectionCount ERROR :", err)
				utils.Logger().Error(err)

			}
			_, er := baseCollection.DeleteMany(dbToContextMap[v.DbName].Context, bson.M{v.Date: bson.M{
				"$lt": currentTime,
			}})
			if er != nil {
				fmt.Println("baseCollectionDelete ERROR : ", er)
				utils.Logger().Panic(err)

			}
			baseDocumentCount, err := baseCollection.CountDocuments(dbToContextMap[v.DbName].Context, bson.D{})
			if err != nil {
				fmt.Println("baseDocumentCount ERROR:", err)
				utils.Logger().Error(err)

			}
			fmt.Printf("%-30s %-15d\n", "INITIAL DOCUMENT COUNT : ", initial_count)
			fmt.Printf("%-30s %-15d\n", "ARCHIVED DOCUMENT COUNT : ", newDocumentCount)
			fmt.Printf("%-30s %-15d\n", "FINAL_DOCUMENT_COUNT : ", baseDocumentCount)

			if v.ColName == "NotificationRecords" {
				if baseDocumentCount+newDocumentCount+isNotSentCount == initial_count {
					fmt.Println("Archived Count(", newDocumentCount, ") + NotSentNotificationRecords Count(", newDocumentCount, ") + Final Count(", baseDocumentCount, ") = Initial Count(", initial_count, ") => Data is Consistent")
					utils.Logger().Info("Success")

				} else {
					utils.Logger().Panic("Document Count Not Matched")
				}
			} else {

				if baseDocumentCount+newDocumentCount == initial_count {
					fmt.Println("Archived Count(", newDocumentCount, ") + Final Count(", baseDocumentCount, ") = Initial Count(", initial_count, ") => Data is Consistent")
					utils.Logger().Info("Success")

				} else {
					utils.Logger().Panic("Document Count Not Matched")
				}
			}
		}
	}
}
func main() {
	utils.InitLogger("Archival_Log.log")
	utils.Logger().Info("Hello")
	log.Println(time.Now())
	configDoc = initConfig()
	ArchiveCollections()
	log.Println(time.Now())
}
