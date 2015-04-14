/**
* This file is part of tingea.  You can redistribute and/or modify tingea
* under the terms of the GNU General Public License; either version 3 of the
* License or (at your option) any later version.  You should have received a
* copy of the GPL along with tingea, in the file COPYING.
*
* The implementation of alloc in Stijn van Dongen's mcl algortihm has been the inspiration of this class.
*
*/
#ifndef nat_alloc_h
#define nat_alloc_h

#include "types.h"

void* mclAlloc (dim size);

void* mclRealloc (void* object, dim new_size);

void mclFree (void* object);

#endif