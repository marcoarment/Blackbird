//
//  BlackbirdModelObjC.m
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

#import "BlackbirdModelObjC.h"
#import <Foundation/Foundation.h>
@import Dispatch;

NSString * const BlackbirdModelTableDidChangeNotification = @"BlackbirdTableChangeNotification";
NSString * const BlackbirdModelChangedTableKey = @"BlackbirdChangedTable";
NSString * const BlackbirdModelChangedPrimaryKeyValuesKey = @"BlackbirdChangedPrimaryKeyValues";

@implementation BlackbirdModelObjC

+ (BlackbirdTableObjC * _Nonnull)table {
    [[NSException exceptionWithName:@"BlackbirdModelObjC" reason:[NSString stringWithFormat:@"+table method not implemented in %@", NSStringFromClass(self)] userInfo:nil] raise];
    return nil;
}

+ (void)resolveInDatabase:(BlackbirdDatabaseObjC * _Nonnull)database completion:(void (^ _Nullable)(void))completion {
    [database resolveWithTable:self.table completionHandler:completion];
}

+ (void)resolveInDatabaseSync:(BlackbirdDatabaseObjC * _Nonnull)database {
    dispatch_semaphore_t semaphore = dispatch_semaphore_create(0);
    [self resolveInDatabase:database completion:^{
        dispatch_semaphore_signal(semaphore);
    }];
    dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER);
}

+ (instancetype _Nullable)readFromDatabaseSync:(BlackbirdDatabaseObjC * _Nonnull)database withID:(id _Nonnull)idValue {
    dispatch_semaphore_t semaphore = dispatch_semaphore_create(0);
    __block BlackbirdModelObjC *result = nil;
    [self readFromDatabase:database withID:idValue completion:^(BlackbirdModelObjC * _Nullable r) {
        result = r;
        dispatch_semaphore_signal(semaphore);
    }];
    dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER);
    return result;
}

+ (void)readFromDatabase:(BlackbirdDatabaseObjC * _Nonnull)database withID:(id _Nonnull)idValue completion:(void (^ _Nullable)(BlackbirdModelObjC * _Nullable))completion {
    [self readFromDatabase:database where:@"id = ?" arguments:@[idValue] completion:^(NSArray *results){
        if (completion) completion(results.firstObject);
    }];
}

+ (void)readFromDatabase:(BlackbirdDatabaseObjC * _Nonnull)database where:(NSString * _Nonnull)where arguments:(NSArray * _Nullable)arguments completion:(void (^ _Nullable)(NSArray<BlackbirdModelObjC *> * _Nonnull))completion {
    BlackbirdTableObjC *table = self.table;
    [database resolveWithTable:table completionHandler:^{
        NSString *query = [NSString stringWithFormat:@"SELECT * FROM `%@` WHERE %@", table.name, where ?: @"1"];
        [database query:query arguments:(arguments ?: @[]) completionHandler:^(NSArray<NSDictionary<NSString *,NSObject *> *> * _Nonnull rows) {
            if (! completion) return;
            
            NSMutableArray<BlackbirdModelObjC *> *results = [NSMutableArray array];
            for (NSDictionary<NSString *, id> *row in rows) {
                BlackbirdModelObjC *instance = [self new];
                [row enumerateKeysAndObjectsUsingBlock:^(NSString * _Nonnull key, id  _Nonnull obj, BOOL * _Nonnull stop) {
                    if (obj == NSNull.null) [instance setValue:nil forKey:key];
                    else [instance setValue:obj forKey:key];
                }];
                [results addObject:instance];
            }
            completion(results);
        }];
    }];
}

+ (NSArray<BlackbirdModelObjC *> * _Nonnull)readFromDatabaseSync:(BlackbirdDatabaseObjC * _Nonnull)database where:(NSString * _Nonnull)where arguments:(NSArray * _Nullable)arguments {
    dispatch_semaphore_t semaphore = dispatch_semaphore_create(0);
    __block NSArray<BlackbirdModelObjC *> *results = nil;
    [self readFromDatabase:database where:where arguments:arguments completion:^(NSArray<BlackbirdModelObjC *> * _Nonnull r) {
        results = r;
        dispatch_semaphore_signal(semaphore);
    }];
    dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER);
    return results;
}

- (void)writeToDatabase:(BlackbirdDatabaseObjC * _Nonnull)database completion:(void (^ _Nullable)(void))completion {
    BlackbirdTableObjC *table = self.class.table;
    [database resolveWithTable:table completionHandler:^{
        NSMutableArray *arguments = [NSMutableArray array];
        NSMutableArray<NSString *> *placeholders = [NSMutableArray array];
        for (NSString *columnName in table.columnNames) {
            [arguments addObject:[self valueForKey:columnName] ?: NSNull.null];
            [placeholders addObject:@"?"];
        }
        
        NSString *query = [NSString stringWithFormat:@"REPLACE INTO `%@` (`%@`) VALUES (%@)",
            table.name,
            [table.columnNames componentsJoinedByString:@"`,`"],
            [placeholders componentsJoinedByString:@","]
        ];
        
        [database query:query arguments:arguments completionHandler:^(NSArray *results){
            if (completion) completion();
        }];
    }];
}

- (void)writeToDatabaseSync:(BlackbirdDatabaseObjC * _Nonnull)database {
    dispatch_semaphore_t semaphore = dispatch_semaphore_create(0);
    [self writeToDatabase:database completion:^{
        dispatch_semaphore_signal(semaphore);
    }];
    dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER);
}

- (void)deleteFromDatabase:(BlackbirdDatabaseObjC * _Nonnull)database completion:(void (^ _Nullable)(void))completion {
    BlackbirdTableObjC *table = self.class.table;
    [database resolveWithTable:table completionHandler:^{
        NSMutableArray *arguments = [NSMutableArray array];
        NSMutableArray<NSString *> *andClauses = [NSMutableArray array];
        for (NSString *columnName in table.primaryKeyColumnNames) {
            [arguments addObject:[self valueForKey:columnName] ?: NSNull.null];
            [andClauses addObject:[NSString stringWithFormat:@"`%@` = ?", columnName]];
        }
        
        NSString *query = [NSString stringWithFormat:@"DELETE FROM `%@` WHERE %@", table.name, [andClauses componentsJoinedByString:@" AND "]];
        [database query:query arguments:arguments completionHandler:^(NSArray *results){
            if (completion) completion();
        }];
    }];
}

- (void)deleteFromDatabaseSync:(BlackbirdDatabaseObjC * _Nonnull)database {
    dispatch_semaphore_t semaphore = dispatch_semaphore_create(0);
    [self deleteFromDatabase:database completion:^{
        dispatch_semaphore_signal(semaphore);
    }];
    dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER);
}

@end
