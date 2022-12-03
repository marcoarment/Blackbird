//
//  BlackbirdModelObjC.h
//  Created by Marco Arment on 11/29/22.
//  Copyright (c) 2022 Marco Arment
//
//  Released under the MIT License
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in all
//  copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//  SOFTWARE.
//
//
//  ***************************************************************************************************
//  *                                                                                                 *
//  *    This file can be omitted from projects that don't need Blackbird access from Objective-C.    *
//  *                                                                                                 *
//  ***************************************************************************************************
//

#ifndef BlackbirdModelObjC_h
#define BlackbirdModelObjC_h

#import <Foundation/Foundation.h>

extern NSString * _Nonnull const BlackbirdModelObjCTableDidChangeNotification;
extern NSString * _Nonnull const BlackbirdModelObjCChangedTableKey;
extern NSString * _Nonnull const BlackbirdModelObjCChangedPrimaryKeyValuesKey;


/// The superclass for Objective-C Blackbird models, providing a basic subset of the functionality of Swift `BlackbirdModel` instances.
@interface BlackbirdModelObjC : NSObject


/// Specifies the table schema for this model. **Required** for subclasses to override.
/// - Returns: A ``BlackbirdTableObjC`` to define the table for this model.
///
+ (BlackbirdTableObjC * _Nonnull)table;


/// Performs setup and any necessary schema migrations.
///
/// Optional. If not called manually, setup and schema migrations will occur when the first database operation is performed by this class.
///
/// - Parameters:
///   - database: The ``BlackbirdDatabaseObjC`` instance to resolve the schema in.
///   - completion: A block to call upon completion. **May be called on a background thread.**
///
+ (void)resolveInDatabase:(BlackbirdDatabaseObjC * _Nonnull)database completion:(void (^ _Nullable)(void))completion;


/// Reads a single instance with the given primary-key value from a database if the primary key is a single column named `id`.
/// - Parameters:
///   - database: The ``BlackbirdDatabaseObjC`` instance to read from.
///   - idValue: The value of the `id` column.
///   - completion: A block to call upon completion. **May be called on a background thread.**
///
+ (void)readFromDatabase:(BlackbirdDatabaseObjC * _Nonnull)database withID:(id _Nonnull)idValue completion:(void (^ _Nullable)(BlackbirdModelObjC * _Nullable))completion;


/// Reads instances from a database using an array of arguments.
///
/// - Parameters:
///   - database: The ``BlackbirdDatabaseObjC`` instance to read from.
///   - where: The portion of the desired SQL query after the `WHERE` keyword. May contain placeholders specified as a question mark (`?`).
///   - arguments: An array of values corresponding to any placeholders in the query.
///   - completion: A block to call upon completion with an array of matching instances. **May be called on a background thread.**
/// - Returns: An array of decoded instances matching the query.
+ (void)readFromDatabase:(BlackbirdDatabaseObjC * _Nonnull)database where:(NSString * _Nonnull)where arguments:(NSArray * _Nullable)arguments completion:(void (^ _Nullable)(NSArray<BlackbirdModelObjC *> * _Nonnull))completion;


/// Write this instance to a database.
/// - Parameters:
///   - database: The ``BlackbirdDatabaseObjC`` instance to write to.
///   - completion: A block to call upon completion. **May be called on a background thread.**
- (void)writeToDatabase:(BlackbirdDatabaseObjC * _Nonnull)database completion:(void (^ _Nullable)(void))completion;


/// Delete this instance from a database.
/// - Parameters:
///   - database: The ``BlackbirdDatabaseObjC`` instance to delete from.
///   - completion: A block to call upon completion. **May be called on a background thread.**
- (void)deleteFromDatabase:(BlackbirdDatabaseObjC * _Nonnull)database completion:(void (^ _Nullable)(void))completion;


/// Synchronous version of ``resolveInDatabase:completion:`` using blocking semaphores.
///
/// > Warning: Deadlock risk if misused. Use the asynchronous functions when possible.
+ (void)resolveInDatabaseSync:(BlackbirdDatabaseObjC * _Nonnull)database;

/// Synchronous version of ``readFromDatabase:withID:completion:`` using blocking semaphores.
///
/// > Warning: Deadlock risk if misused. Use the asynchronous functions when possible.
+ (instancetype _Nullable)readFromDatabaseSync:(BlackbirdDatabaseObjC * _Nonnull)database withID:(id _Nonnull)idValue;

/// Synchronous version of ``readFromDatabase:where:arguments:completion:`` using blocking semaphores.
///
/// > Warning: Deadlock risk if misused. Use the asynchronous functions when possible.
+ (NSArray<BlackbirdModelObjC *> * _Nonnull)readFromDatabaseSync:(BlackbirdDatabaseObjC * _Nonnull)database where:(NSString * _Nonnull)where arguments:(NSArray * _Nullable)arguments;

/// Synchronous version of ``writeToDatabase:completion:`` using blocking semaphores.
///
/// > Warning: Deadlock risk if misused. Use the asynchronous functions when possible.
- (void)writeToDatabaseSync:(BlackbirdDatabaseObjC * _Nonnull)database;

/// Synchronous version of ``deleteFromDatabase:completion:`` using blocking semaphores.
///
/// > Warning: Deadlock risk if misused. Use the asynchronous functions when possible.
- (void)deleteFromDatabaseSync:(BlackbirdDatabaseObjC * _Nonnull)database;

@end

#endif /* BlackbirdModelObjC_h */
