package org.apache.spark.sql.cassandra

import com.datastax.spark.connector.util.Logging
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.CassandraSession.CassandraSharedState
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.internal.SharedState

class CassandraSession(sc: SparkContext) extends SparkSession(sc) {
  override protected[sql] lazy val sharedState: SharedState = new CassandraSharedState(sc)
}

object CassandraSession {

  class CassandraSharedState(sc: SparkContext) extends SharedState(sc) {
    override lazy val externalCatalog: ExternalCatalog = new CassandraCatalog(sc)
  }

  class CassandraCatalog(sc: SparkContext) extends ExternalCatalog with Logging {
    override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
      logInfo(s"createDatabase(dbDefinition: $dbDefinition, ignoreIfExists: $ignoreIfExists)")
    }

    override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = ???

    override def alterDatabase(dbDefinition: CatalogDatabase): Unit = ???

    override def getDatabase(db: String): CatalogDatabase = ???

    override def databaseExists(db: String): Boolean = ???

    override def listDatabases(): Seq[String] = ???

    override def listDatabases(pattern: String): Seq[String] = ???

    override def setCurrentDatabase(db: String): Unit = ???

    override def createTable(db: String, tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = ???

    override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean): Unit = ???

    override def renameTable(db: String, oldName: String, newName: String): Unit = ???

    override def alterTable(db: String, tableDefinition: CatalogTable): Unit = ???

    override def getTable(db: String, table: String): CatalogTable = {
      throw new NotImplementedError(s"getTabe(db=$db, table=$table")
    }

    override def getTableOption(db: String, table: String): Option[CatalogTable] = ???

    override def tableExists(db: String, table: String): Boolean = ???

    override def listTables(db: String): Seq[String] = ???

    override def listTables(db: String, pattern: String): Seq[String] = ???

    override def loadTable(db: String, table: String, loadPath: String, isOverwrite: Boolean, holdDDLTime: Boolean): Unit = ???

    override def loadPartition(db: String, table: String, loadPath: String, partition: TablePartitionSpec, isOverwrite: Boolean, holdDDLTime: Boolean, inheritTableSpecs: Boolean, isSkewedStoreAsSubdir: Boolean): Unit = ???

    override def createPartitions(db: String, table: String, parts: Seq[CatalogTablePartition], ignoreIfExists: Boolean): Unit = ???

    override def dropPartitions(db: String, table: String, parts: Seq[TablePartitionSpec], ignoreIfNotExists: Boolean): Unit = ???

    override def renamePartitions(db: String, table: String, specs: Seq[TablePartitionSpec], newSpecs: Seq[TablePartitionSpec]): Unit = ???

    override def alterPartitions(db: String, table: String, parts: Seq[CatalogTablePartition]): Unit = ???

    override def getPartition(db: String, table: String, spec: TablePartitionSpec): CatalogTablePartition = ???

    override def listPartitions(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = ???

    override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = ???

    override def dropFunction(db: String, funcName: String): Unit = ???

    override def renameFunction(db: String, oldName: String, newName: String): Unit = ???

    override def getFunction(db: String, funcName: String): CatalogFunction = ???

    override def functionExists(db: String, funcName: String): Boolean = ???

    override def listFunctions(db: String, pattern: String): Seq[String] = ???
  }
}

