
import ABNative
import ABDatabase
import Foundation

public class ABDatabaseNative: ABNativeActionsSet {
    
    public var db: ABDatabase
    public var nativeActions: ABNativeActionsSet
    
    public init(_ db: ABDatabase) {
        self.db = db
        self.nativeActions = ABNativeActionsSet()
        
        super.init()
        
        self
            .addNativeCallback("GetTableColumnInfos") { args, onResult, onError in
                guard let args else {
                    onError(ABDatabaseNativeError.cannotParseJSON("Args not set."))
                    return
                }
                
                guard let tableName = args["tableName"] as? String else {
                    onError(ABDatabaseNativeError.cannotParseJSON("'tableName' not set."))
                    return
                }
                
                var transactionId: Int?
                if args["transactionId"] is NSNull {
                    transactionId = nil
                } else {
                    guard let v = args["transactionId"] as? Int else {
                        onError(ABDatabaseNativeError.cannotParseJSON("'transactionId' not set."))
                        return
                    }
                    transactionId = v
                }
                
                db.getTableColumnInfos(tableName) { columnInfos in
                    var rColumnInfos = [AnyObject]()
                    columnInfos.forEach { columnInfo in
                        var rColumnInfo = [String: AnyObject]()
                        rColumnInfo["name"] = columnInfo.name as AnyObject
                        rColumnInfo["type"] = columnInfo.type as AnyObject
                        rColumnInfo["notNull"] = columnInfo.notNull as AnyObject
                        
                        rColumnInfos.append(rColumnInfo as AnyObject)
                    }
                    
                    var result = [String: AnyObject]()
                    result["columnInfos"] = rColumnInfos as AnyObject

                    onResult(result)
                } execute: { error in
                    onError(error)
                }

            }
            .addNativeCallback("GetTableNames", execute: { args, onResult, onError in
                guard let args else {
                    onError(ABDatabaseNativeError.cannotParseJSON("Args not set."))
                    return
                }
                
                var transactionId: Int?
                if args["transactionId"] is NSNull {
                    transactionId = nil
                } else {
                    guard let v = args["transactionId"] as? Int else {
                        onError(ABDatabaseNativeError.cannotParseJSON("'transactionId' not set."))
                        return
                    }
                    transactionId = v
                }
                
                db.getTableNames(transactionId: transactionId) { tableNames in
                    var rTableNames = [AnyObject]()
                    tableNames.forEach { tableName in
                        rTableNames.append(tableName as AnyObject)
                    }
                    
                    var result = [String: AnyObject]()
                    result["tableNames"] = rTableNames as AnyObject
                    
                    onResult(result)
                } execute: { error in
                    onError(error)
                }
            })
            .addNativeCallback("Transaction_Finish", execute: { args, onResult, onError in
                guard let args else {
                    onError(ABDatabaseNativeError.cannotParseJSON("Args not set."))
                    return
                }
                
                guard let transactionId = args["transactionId"] as? Int else {
                    onError(ABDatabaseNativeError.cannotParseJSON("'transactionId' not set."))
                    return
                }
                
                guard let commit = args["commit"] as? Bool else {
                    onError(ABDatabaseNativeError.cannotParseJSON("'commit' not set."))
                    return
                }
                
                db.transaction_Finish(transactionId, commit) {
                    onResult(nil)
                } execute: { error in
                    onError(error)
                }
            })
            .addNativeCallback("Transaction_IsAutocommit", execute: { args, onResult, onError in
                db.transaction_IsAutocommit { transactionId in
                    var result = [String: AnyObject]()
                    result["transactionId"] = transactionId as AnyObject ?? NSNull()
                    
                    onResult(result)
                } execute: { error in
                    onError(error)
                }
            })
            .addNativeCallback("Transaction_Start", execute: { args, onResult, onError in
                db.transaction_Start { transactionId in
                    var result = [String: AnyObject]()
                    result["transactionId"] = transactionId as AnyObject ?? NSNull()
                    
                    onResult(result)
                } execute: { error in
                    onError(error)
                }
            })
            .addNativeCallback("Query_Execute", execute: { args, onResult, onError in
                guard let args else {
                    onError(ABDatabaseNativeError.cannotParseJSON("Args not set."))
                    return
                }
                
                var transactionId: Int?
                if args["transactionId"] is NSNull {
                    transactionId = nil
                } else {
                    guard let v = args["transactionId"] as? Int else {
                        onError(ABDatabaseNativeError.cannotParseJSON("'transactionId' not set."))
                        return
                    }
                    transactionId = v
                }
                
                guard let query = args["query"] as? String else {
                    onError(ABDatabaseNativeError.cannotParseJSON("'query' not set."))
                    return
                }
                
                db.query_Execute(query, transactionId: transactionId) {
                    onResult(nil)
                } execute: { error in
                    onError(error)
                }
            })
            .addNativeCallback("Query_Select", execute: { args, onResult, onError in
                guard let args else {
                    onError(ABDatabaseNativeError.cannotParseJSON("Args not set."))
                    return
                }
                
                var transactionId: Int?
                if args["transactionId"] is NSNull {
                    transactionId = nil
                } else {
                    guard let v = args["transactionId"] as? Int else {
                        onError(ABDatabaseNativeError.cannotParseJSON("'transactionId' not set."))
                        return
                    }
                    transactionId = v
                }
                
                guard let query = args["query"] as? String else {
                    onError(ABDatabaseNativeError.cannotParseJSON("'query' not set."))
                    return
                }
                
                guard let args_ColumnTypes = args["columnTypes"] as? [Int] else {
                    onError(ABDatabaseNativeError.cannotParseJSON("'columnTypes' not set."))
                    return
                }
                
                var columnTypes = [SelectColumnType]()
                do {
                    try args_ColumnTypes.forEach { i in
                        columnTypes.append(try SelectColumnType.fromIndex(i))
                    }
                } catch {
                    onError(error)
                    return
                }
                
                db.query_Select(query, columnTypes, transactionId: transactionId) { rows in
                    var result = [String: AnyObject]()
                    result["rows"] = rows as AnyObject
                    onResult(result)
                } execute: { error in
                    onError(error)
                }
            })
    }
    
    
    public func error(_ message: String) {
        print("ABDatabase Error -> \(message)")
    }
    
}


public enum ABDatabaseNativeError: Error {
    case cannotParseJSON(_ message: String)
    case unknownColumnType(_ columnType: String)
}
