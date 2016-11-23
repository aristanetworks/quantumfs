// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#ifndef QFS_API_INTERNAL_H
#define QFS_API_INTERNAL_H

#include <vector>

using namespace std;

namespace qfsclient {
   
   void split(const string& str,
              const string& delimiters,
              vector<string>& tokens);
   
   void join(const vector<string>& pieces, const string& glue, string& result);
   
}

#endif // QFS_API_INTERNAL_H

