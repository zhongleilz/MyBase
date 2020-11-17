//
// Created by zhong on 2020/11/9.
//

#ifndef RM_INTERNAL_H
#define RM_INTERNAL_H

#include <string>
#include "rm.h"

#define RM_NO_FREE_PAGE -1


struct RM_PageHeader{
        PageNum nextPage;
};


#endif //MYBASE_RM_INTERNAL_H
