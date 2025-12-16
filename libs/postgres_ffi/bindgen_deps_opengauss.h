/*
 * Wrapper header for bindgen to generate openGauss bindings.
 * It includes the minimal set of openGauss headers we need for WAL/XLOG types.
 */
#include "c.h"
#include "catalog/pg_control.h"
#include "access/xlogrecord.h"

#include "storage/block.h"
#include "storage/bufpage.h"
#include "storage/off.h"
#include "access/multixact.h"
