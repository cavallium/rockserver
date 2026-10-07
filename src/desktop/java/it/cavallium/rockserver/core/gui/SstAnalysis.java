package it.cavallium.rockserver.core.gui;

import it.cavallium.rockserver.core.common.SstMaintenance;
import java.awt.geom.Rectangle2D;
import java.util.*;
import java.util.function.ToIntFunction;

/** Physical size statistics and deterministic, area-preserving treemap geometry. */
public final class SstAnalysis {
    private SstAnalysis() {}

    public record Stats(int files, long bytes, long smallest, double median, long largest, int compacting) {
        public double largestToMedian() { return median > 0 ? largest / median : Double.NaN; }
    }

    public static Stats summarize(List<SstMaintenance.File> files) {
        long[] sizes = files.stream().mapToLong(SstMaintenance.File::sizeBytes).sorted().toArray();
        long total = 0;
        int compacting = 0;
        for (var file : files) {
            if (file.sizeBytes() < 0) throw new IllegalArgumentException("Negative SST size");
            total = Math.addExact(total, file.sizeBytes());
            if (file.beingCompacted()) compacting++;
        }
        int n = sizes.length;
        double median = n == 0 ? 0 : n % 2 == 0 ? sizes[n / 2 - 1] / 2d + sizes[n / 2] / 2d : sizes[n / 2];
        return new Stats(n, total, n == 0 ? 0 : sizes[0], median, n == 0 ? 0 : sizes[n - 1], compacting);
    }

    /** Include empty levels/paths, preserving their actual IDs. */
    public static List<Stats> grouped(List<SstMaintenance.File> files, int count, ToIntFunction<SstMaintenance.File> id) {
        var groups = new ArrayList<List<SstMaintenance.File>>(count);
        for (int i = 0; i < count; i++) groups.add(new ArrayList<>());
        for (var file : files) {
            int index = id.applyAsInt(file);
            if (index < 0 || index >= count) throw new IllegalArgumentException("SST group ID outside metadata: " + index);
            groups.get(index).add(file);
        }
        return groups.stream().map(SstAnalysis::summarize).toList();
    }

    /** Immutable observation index: sort and compute full-column statistics only once. */
    public record Index(SstMaintenance.Metadata metadata, List<SstMaintenance.File> bySize, Stats total,
                        List<Stats> levels, List<Stats> paths, List<List<SstMaintenance.File>> levelFiles) {}

    public static Index index(SstMaintenance.Metadata metadata) {
        var sorted = metadata.files().stream().sorted(Comparator.comparingLong(SstMaintenance.File::sizeBytes).reversed()
                .thenComparing(SstMaintenance.File::name)).toList();
        var levelFiles = new ArrayList<List<SstMaintenance.File>>();
        for (int i = 0; i < metadata.numLevels(); i++) levelFiles.add(new ArrayList<>());
        for (var file : sorted) {
            if (file.level() < 0 || file.level() >= metadata.numLevels()) throw new IllegalArgumentException("Invalid SST level");
            levelFiles.get(file.level()).add(file);
        }
        return new Index(metadata, sorted, summarizeInSizeOrder(sorted),
                levelFiles.stream().map(SstAnalysis::summarizeInSizeOrder).toList(),
                groupedInSizeOrder(sorted, metadata.paths().size(), SstMaintenance.File::pathId),
                levelFiles.stream().map(List::copyOf).toList());
    }

    /** Input is descending by size; filtering an indexed list preserves this order. */
    public static Stats summarizeInSizeOrder(List<SstMaintenance.File> files) {
        long bytes = 0; int compacting = 0;
        for (var file : files) {
            if (file.sizeBytes() < 0) throw new IllegalArgumentException("Negative SST size");
            bytes = Math.addExact(bytes, file.sizeBytes());
            if (file.beingCompacted()) compacting++;
        }
        int n = files.size();
        double median = n == 0 ? 0 : n % 2 == 0 ? files.get(n / 2 - 1).sizeBytes() / 2d + files.get(n / 2).sizeBytes() / 2d : files.get(n / 2).sizeBytes();
        return new Stats(n, bytes, n == 0 ? 0 : files.getLast().sizeBytes(), median,
                n == 0 ? 0 : files.getFirst().sizeBytes(), compacting);
    }

    public static List<Stats> groupedInSizeOrder(List<SstMaintenance.File> files, int count, ToIntFunction<SstMaintenance.File> id) {
        var groups = new ArrayList<List<SstMaintenance.File>>(count);
        for (int i = 0; i < count; i++) groups.add(new ArrayList<>());
        for (var file : files) {
            int index = id.applyAsInt(file);
            if (index < 0 || index >= count) throw new IllegalArgumentException("SST group ID outside metadata: " + index);
            groups.get(index).add(file);
        }
        return groups.stream().map(SstAnalysis::summarizeInSizeOrder).toList();
    }

    public static final int MAX_MAP_TILES = 256;
    /** Groups conserve every file and byte; grouping is visual only, never inventory truncation. */
    public record Tile(List<SstMaintenance.File> files, long bytes, int level, int path, int compacting) {
        public Tile { files = List.copyOf(files); }
        public String label() { return files.size() == 1 ? files.getFirst().name() : (level < 0 ? "" : "L" + level + " · ")
                + (path < 0 ? "" : "Path " + path + " · ") + files.size() + " SSTs"; }
    }

    public static List<Tile> tiles(List<SstMaintenance.File> sortedFiles) {
        if (sortedFiles.size() > MAX_MAP_TILES) {
            var levels = new LinkedHashMap<Integer, List<SstMaintenance.File>>();
            for (var file : sortedFiles) levels.computeIfAbsent(file.level(), key -> new ArrayList<>()).add(file);
            if (levels.size() > 1 && levels.size() <= 64) return levels.values().stream().map(SstAnalysis::tile).toList();
            var paths = new LinkedHashMap<Integer, List<SstMaintenance.File>>();
            for (var file : sortedFiles) paths.computeIfAbsent(file.pathId(), key -> new ArrayList<>()).add(file);
            if (paths.size() > 1 && paths.size() <= 64) return paths.values().stream().map(SstAnalysis::tile).toList();
        }
        var result = new ArrayList<Tile>();
        int singles = sortedFiles.size() <= MAX_MAP_TILES ? sortedFiles.size() : 64;
        for (int i = 0; i < singles; i++) result.add(tile(sortedFiles.subList(i, i + 1)));
        if (singles == sortedFiles.size()) return List.copyOf(result);
        int groupSize = Math.max(1, (sortedFiles.size() - singles + MAX_MAP_TILES - singles - 1) / (MAX_MAP_TILES - singles));
        for (int i = singles; i < sortedFiles.size(); i += groupSize) result.add(tile(sortedFiles.subList(i, Math.min(sortedFiles.size(), i + groupSize))));
        return List.copyOf(result);
    }

    private static Tile tile(List<SstMaintenance.File> files) {
        var stats = summarizeInSizeOrder(files);
        int level = files.getFirst().level(), path = files.getFirst().pathId();
        for (var file : files) {
            if (file.level() != level) level = -1;
            if (file.pathId() != path) path = -1;
        }
        return new Tile(files, stats.bytes(), level, path, stats.compacting());
    }

    public static String size(double bytes) {
        String[] units = {"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"};
        int unit = 0;
        while (bytes >= 1024 && unit < units.length - 1) { bytes /= 1024; unit++; }
        return String.format(Locale.ROOT, unit == 0 ? "%.0f %s" : "%.1f %s", bytes, units[unit]);
    }

    /** Result indices match input indices; zero weights receive empty rectangles, never invented area. */
    public static List<Rectangle2D.Double> treemap(List<Long> weights, double width, double height) {
        if (!Double.isFinite(width) || !Double.isFinite(height) || width < 0 || height < 0) {
            throw new IllegalArgumentException("Invalid treemap dimensions");
        }
        var result = new ArrayList<Rectangle2D.Double>(weights.size());
        var order = new ArrayList<Integer>();
        for (int i = 0; i < weights.size(); i++) {
            if (weights.get(i) < 0) throw new IllegalArgumentException("Negative weight");
            result.add(new Rectangle2D.Double());
            if (weights.get(i) > 0) order.add(i);
        }
        order.sort(Comparator.<Integer>comparingLong(weights::get).reversed());
        double[] prefix = new double[order.size() + 1];
        for (int i = 0; i < order.size(); i++) prefix[i + 1] = prefix[i] + weights.get(order.get(i));
        if (!order.isEmpty()) split(order, prefix, 0, order.size(), new Rectangle2D.Double(0, 0, width, height), result);
        return result;
    }

    private static void split(List<Integer> order, double[] sums, int from, int to, Rectangle2D.Double r,
                              List<Rectangle2D.Double> result) {
        if (to - from == 1) { result.set(order.get(from), r); return; }
        double total = sums[to] - sums[from];
        double half = sums[from] + total / 2;
        int low = from + 1, high = to - 1;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (sums[mid] < half) low = mid + 1; else high = mid;
        }
        int cut = total > 0 ? low : (from + to) >>> 1;
        if (total > 0 && cut > from + 1 && Math.abs(sums[cut - 1] - half) < Math.abs(sums[cut] - half)) cut--;
        double fraction = total > 0 ? (sums[cut] - sums[from]) / total : (double) (cut - from) / (to - from);
        if (r.width >= r.height) {
            double a = r.width * fraction;
            split(order, sums, from, cut, new Rectangle2D.Double(r.x, r.y, a, r.height), result);
            split(order, sums, cut, to, new Rectangle2D.Double(r.x + a, r.y, r.width - a, r.height), result);
        } else {
            double a = r.height * fraction;
            split(order, sums, from, cut, new Rectangle2D.Double(r.x, r.y, r.width, a), result);
            split(order, sums, cut, to, new Rectangle2D.Double(r.x, r.y + a, r.width, r.height - a), result);
        }
    }
}
