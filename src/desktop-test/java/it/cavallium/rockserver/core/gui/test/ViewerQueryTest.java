package it.cavallium.rockserver.core.gui.test;

import it.cavallium.rockserver.core.gui.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ViewerQueryTest {
    private final ColumnSchema mixed = ColumnSchema.of(IntList.of(2), ObjectList.of(ColumnHashType.ALLSAME8), true);

    @Test void parsesLogicalVariableKeysAndValidatesFixedLengths() {
        Keys keys = ViewerQuery.parseKey(mixed, "00 ff ; 616263");
        assertArrayEquals(new byte[]{0, (byte) 255}, keys.keys()[0].toByteArray());
        assertArrayEquals(new byte[]{97, 98, 99}, keys.keys()[1].toByteArray());
        assertEquals(0, ViewerQuery.parseKey(mixed, "0000;-").keys()[1].size());
        assertNull(ViewerQuery.parseKey(mixed, "  "));
        assertThrows(IllegalArgumentException.class, () -> ViewerQuery.parseKey(mixed, "00;ab"));
        assertThrows(IllegalArgumentException.class, () -> ViewerQuery.parseKey(mixed, "0000;a"));
        assertThrows(IllegalArgumentException.class, () -> ViewerQuery.parseKey(mixed, "0000;zz"));
        assertThrows(IllegalArgumentException.class, () -> ViewerQuery.parseKey(mixed, "0000;"));
        assertThrows(IllegalArgumentException.class, () -> ViewerQuery.parseKey(mixed, "0000"));
    }

    @Test void mixedSchemaUsesAbsoluteVariableKeyIndexAndExposesKeyOnlyRows() {
        assertEquals(List.of("Key 0 · 2 bytes", "Key 1 · variable / ALLSAME8", "Value"), ViewerQuery.columns(mixed));
        var schema = ColumnSchema.of(IntList.of(2), ObjectList.of(), false);
        Keys key = ViewerQuery.parseKey(schema, "1234");
        var page = new RangePage<>(List.of(new KV(key, null)), key, false);
        Object[][] rows = ViewerQuery.rows(schema, page);
        assertEquals(1, rows.length);
        assertEquals(1, rows[0].length);
        assertArrayEquals(new byte[]{0x12, 0x34}, (byte[]) rows[0][0]);
    }

    @Test void boundedQueriesPreserveBoundsDirectionAndContinuationWithoutCachePollution() {
        var api = mock(RocksDBSyncAPI.class);
        for (boolean reverse : List.of(false, true)) {
            var query = ViewerQuery.parse(mixed, "0001;61", "0002;7a", reverse, 100);
            var resume = ViewerQuery.parseKey(mixed, "0001;62");
            query.load(api, 42, resume);
            verify(api).getRangePage(eq(0L), eq(42L), eq(query.start()), eq(query.end()), eq(reverse), eq(resume),
                    isA(RequestType.RequestGetAllInRangeNoCache.class), eq(new RangeBudget(100, 8L * 1024 * 1024)));
        }
        verifyNoMoreInteractions(api);
    }

    @Test void rejectsBudgetsOutsideTheServerContract() {
        assertThrows(IllegalArgumentException.class, () -> ViewerQuery.parse(mixed, "", "", false, 0));
        assertThrows(IllegalArgumentException.class, () -> ViewerQuery.parse(mixed, "", "", false, RangeBudget.DEFAULT_MAX_ITEMS + 1));
    }

    @Test void nullableValuesAndEmptyPagesRemainInspectable() {
        Keys key = ViewerQuery.parseKey(mixed, "0000;61");
        var page = new RangePage<>(List.of(new KV(key, null)), key, false);
        assertNull(ViewerQuery.rows(mixed, page)[0][2]);
        assertEquals(0, ViewerQuery.rows(mixed, RangePage.empty()).length);
    }
}
