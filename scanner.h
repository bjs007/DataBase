#ifndef SCANNER_H_INCLUDED
#define SCANNER_H_INCLUDED
#include "record_mgr.h"

void update(int pageno,Schema *schema,TableManager *tableManager, Pager *pageDet );
void pop(TableManager *tableManager, Pager *pageDet, int pageno);
void push(TableManager *tableManager, Pager *pageDet, int pageno);

#endif // SCANNER_H_INCLUDED
