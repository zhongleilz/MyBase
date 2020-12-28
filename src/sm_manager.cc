#include <cstdio>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include "redbase.h"
#include "sm.h"
#include "ix.h"
#include "rm.h"
#include "ex.h"
#include "printer.h"
#include "parser.h"
using namespace std;

SM_Manager::SM_Manager(IX_Manager & ixm, RM_Manager &rmm){
    this->rmManager = &rmm;
    this->ixManager = &ixm;

    isOpen = FALSE;

    printCommands = FALSE;
    optimizeQuery = TRUE;
    partitionedPrint = FALSE;
}