//
//  TestModels.m
//  Created by Marco Arment on 11/30/22.
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

#import "TestModels.h"

@implementation TestModel
+ (BlackbirdTableObjC *)table {
    return [BlackbirdTableObjC
        tableWithName:NSStringFromClass(self)
        columns:@[
            [BlackbirdColumnObjC columnWithName:@"id"    type:BlackbirdColumnTypeObjCInteger mayBeNull:NO],
            [BlackbirdColumnObjC columnWithName:@"title" type:BlackbirdColumnTypeObjCText mayBeNull:YES],
            [BlackbirdColumnObjC columnWithName:@"score" type:BlackbirdColumnTypeObjCDouble mayBeNull:NO],
            [BlackbirdColumnObjC columnWithName:@"art"   type:BlackbirdColumnTypeObjCData mayBeNull:YES],
        ]
        primaryKeyColumnNames:@[ @"id" ]
        indexes:@[
            [BlackbirdIndexObjC indexWithColumNames:@[ @"title" ] unique:NO],
        ]
    ];
}
@end

@implementation TestModelSchemaAddColumnInitial
+ (BlackbirdTableObjC *)table {
    return [BlackbirdTableObjC
        tableWithName:@"TestModelSchemaAddColumn"
        columns:@[
            [BlackbirdColumnObjC columnWithName:@"id"      type:BlackbirdColumnTypeObjCInteger mayBeNull:NO],
            [BlackbirdColumnObjC columnWithName:@"title"   type:BlackbirdColumnTypeObjCText mayBeNull:YES],
        ]
        primaryKeyColumnNames:@[ @"id" ]
        indexes:@[
        ]
    ];
}

@end

@implementation TestModelSchemaAddColumnChanged
+ (BlackbirdTableObjC *)table {
    return [BlackbirdTableObjC
        tableWithName:@"TestModelSchemaAddColumn"
        columns:@[
            [BlackbirdColumnObjC columnWithName:@"id"      type:BlackbirdColumnTypeObjCInteger mayBeNull:NO],
            [BlackbirdColumnObjC columnWithName:@"title"   type:BlackbirdColumnTypeObjCText mayBeNull:YES],
            [BlackbirdColumnObjC columnWithName:@"summary" type:BlackbirdColumnTypeObjCText mayBeNull:YES],
        ]
        primaryKeyColumnNames:@[ @"id" ]
        indexes:@[
        ]
    ];
}
@end
