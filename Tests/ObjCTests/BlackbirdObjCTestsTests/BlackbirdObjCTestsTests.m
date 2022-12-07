//
//  BlackbirdObjCTestsTests.m
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

#import <XCTest/XCTest.h>
#import "BlackbirdModelObjC.h"
#import "TestModels.h"

@interface BlackbirdObjCTestsTests : XCTestCase {
    NSString *sqliteFilename;
}
@end

@implementation BlackbirdObjCTestsTests

- (void)setUp {
    uint64_t random;
    XCTAssert(0 == SecRandomCopyBytes(NULL, sizeof(uint64_t), &random));
    
    NSString *tmpDir = NSSearchPathForDirectoriesInDomains(NSCachesDirectory, NSUserDomainMask, YES)[0];
    sqliteFilename = [tmpDir stringByAppendingPathComponent:[NSString stringWithFormat:@"test%llu.sqlite", (unsigned long long) random]];
}

- (void)tearDown {
    if (sqliteFilename && ! [sqliteFilename isEqualToString:@":memory:"]) {
        [NSFileManager.defaultManager removeItemAtPath:sqliteFilename error:NULL];
    }
}

- (void)testBasics {
    BlackbirdDatabaseObjC *db = [[BlackbirdDatabaseObjC alloc] initWithPath:sqliteFilename debugLogging:YES];

    TestModel *t = [TestModel new];
    t.id = 123456;
    t.title = @"Test title!";
    t.score = 3.14159265;
    t.art = [@"hi🎨" dataUsingEncoding:NSUTF8StringEncoding];
    [t writeToDatabaseSync:db];
    
    
    t = [TestModel readFromDatabaseSync:db withID:@123];
    XCTAssertNil(t);
    
    t = [TestModel readFromDatabaseSync:db withID:@123456];
    XCTAssertNotNil(t);
    
    XCTAssert(t.id == 123456);
    XCTAssert([t.title isEqualToString:@"Test title!"]);
    XCTAssert(t.score == 3.14159265);
    XCTAssert([t.art isEqualToData:[@"hi🎨" dataUsingEncoding:NSUTF8StringEncoding]]);
}

- (void)testSchemaChangeAddColumn {
    BlackbirdDatabaseObjC *db = [[BlackbirdDatabaseObjC alloc] initWithPath:sqliteFilename debugLogging:YES];

    TestModelSchemaAddColumnInitial *a = [TestModelSchemaAddColumnInitial new];
    a.id = 7890;
    a.title = @"Test title!";
    [a writeToDatabaseSync:db];
    [db closeSync];
    
    db = [[BlackbirdDatabaseObjC alloc] initWithPath:sqliteFilename debugLogging:YES];
    TestModelSchemaAddColumnChanged *b = [TestModelSchemaAddColumnChanged readFromDatabaseSync:db withID:@7890];
    XCTAssertNotNil(b);
    XCTAssert([b.title isEqualToString:a.title]);
    XCTAssert(b.summary == nil);
    
    b.summary = @"Summary!";
    [b writeToDatabaseSync:db];
    
    TestModelSchemaAddColumnChanged *c = [TestModelSchemaAddColumnChanged readFromDatabaseSync:db withID:@7890];
    XCTAssertNotNil(c);
    XCTAssert([c.title isEqualToString:a.title]);
    XCTAssert([c.summary isEqualToString:@"Summary!"]);
}

- (void)testSchemaChangeDropColumn {
    BlackbirdDatabaseObjC *db = [[BlackbirdDatabaseObjC alloc] initWithPath:sqliteFilename debugLogging:YES];

    TestModelSchemaAddColumnChanged *a = [TestModelSchemaAddColumnChanged new];
    a.id = 7890;
    a.title = @"Test title!";
    a.summary = @"Summary!";
    [a writeToDatabaseSync:db];
    [db closeSync];
    
    db = [[BlackbirdDatabaseObjC alloc] initWithPath:sqliteFilename debugLogging:YES];
    TestModelSchemaAddColumnInitial *b = [TestModelSchemaAddColumnInitial readFromDatabaseSync:db withID:@7890];
    XCTAssertNotNil(b);
    XCTAssert([b.title isEqualToString:a.title]);
}

@end


