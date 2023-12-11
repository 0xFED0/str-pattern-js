const DEF_MIN_MATCH_RATIO = 0.15;
const DEF_MAX_DIST = 10;
const DEF_MASK_CHAR = "•";
const DEF_SEQ_MIN_SIZE = 3;
const DEF_MIN_GROUP_RATIO = 0.2;
const DEF_MAX_RATIO_RANGE = 0.5;
const DEF_RATIO_REL_TOL = 0.3;
const DEF_LEN_DIFF_MAX_RATIO = 0.4;
const DEF_MAX_CROSSPATTERN_MATCH = 0.5;
const DEF_MAX_UNIQUE_MATCH_RATIO = 0.75;
const DEF_MIN_CONST_PART_RATIO = 0.15;
const DEF_MIN_GOOD_MATCHED_STAT = 0.01;

const FIELD_NONE = null;

async function test() {
    const fs = require("fs");
    const yaml = require("js-yaml");

    let data = fs.readFileSync(__dirname + '/messages.yaml');
    let msgs = yaml.load(data.toString());
    data = null;

    tool = new Patterns();

    if(fs.existsSync(__dirname + '/patterns.json')) {
        data = fs.readFileSync(__dirname + '/patterns.json');
        data = JSON.parse(data.toString());
        tool.load(data);
        data = null;
    }

    tool.putMessages(msgs);
    
    data = tool.dump();
    data = JSON.stringify(data, null, '\t');
    fs.writeFileSync(__dirname + '/patterns.json', data);
}

class Patterns {
    options = {
        mask_char: DEF_MASK_CHAR,
        min_match_ratio: DEF_MIN_MATCH_RATIO,
        maxdist: DEF_MAX_DIST,
        sequence_minlen: DEF_SEQ_MIN_SIZE,
        min_group_ratio: DEF_MIN_GROUP_RATIO,
        max_ratio_range: DEF_MAX_RATIO_RANGE,
        ratio_rel_tol: DEF_RATIO_REL_TOL,
        len_diff_max_ratio: DEF_LEN_DIFF_MAX_RATIO,
        max_crosspattern_match: DEF_MAX_CROSSPATTERN_MATCH,
        max_unique_match_ratio: DEF_MAX_UNIQUE_MATCH_RATIO,
        min_const_part_ratio: DEF_MIN_CONST_PART_RATIO,
        min_good_matched_stat: DEF_MIN_GOOD_MATCHED_STAT,
        no_fast_search_prepass: false,
    }
    patterns = [];
    total_messages = 0;

    constructor(opt=null) {
        this.options = {
            ...this.options,
            ...opt
        }
    }

    /**
     * Apply messages to correct or generate patterns
     * @param {Array<string>} messages bunch of messages
     * @returns {Object} { updated, added } each field contains Array<{pattern, msg_indexes}>
    */
    putMessages(messages) {
        for(let i = 0; i < messages.length; i++) {
            this.#putMsgToPatterns(messages[i], i);
        }
        this.total_messages += messages.length;
        this.#correctPatterns();
        let result = this.#getChangesReport();
        this.#cleanupPatterns();
        return result;
    }

    dump() {
        return {
            mask_char: this.options.mask_char,
            total_messages: this.total_messages,
            patterns: this.patterns.map(p => { return {...p} }),
        }
    }

    load(jsobj, append=false) {
        jsobj = this.#adaptLoadData(jsobj);
        if(append) {
            appendToArray(this.patterns, jsobj.patterns);
            this.total_messages += jsobj.total_messages;
        } else {
            this.patterns = jsobj.patterns;
            this.total_messages = jsobj.total_messages;
        }
    }

    #putMsgToPatterns(msg, i) {
        let r = closestPatternForMessage(this.patterns, msg, this.options);
        if((r != null) && !this.#acceptablePattern(r))
            r = null;
        let rec = (r != null) ? this.patterns[r.index] : {
            is_new: true,
            pattern: msg,
            stats: { checked: 1, hits: 1, matched: 1 },
        }
        rec.msg_indexes = rec.msg_indexes || [i];
        if(r == null)
            this.patterns.push(rec);
        else {
            rec.is_updated = true;
            rec.stats.matched++;
            rec.pattern = r.corrected_pattern;
            rec.msg_indexes.push(i);
        }
    }

    #correctPatterns() {
        let total_messages = this.total_messages;
        let average_matched = total_messages / this.patterns.length;
        function calc_rating(stats) {
            return 1.00 * stats.matched / total_messages
            + 0.75 * stats.matched / average_matched
            + 0.50 * stats.matched / (stats.checked || 1)
            + 0.25 * stats.matched / (stats.hits || 1)
        }
        for(let pat of this.patterns)
            pat.stats.rating = calc_rating(pat.stats);
        
        const { min_good_matched_stat } = this.options;
        function is_bad(pat) {
            if(pat == null)
                return false;
            return pat.stats.rating < min_good_matched_stat;
        }
        let bad_patterns = [];
        for(let i = 0; i < this.patterns.length; i++) {
            let pat = this.patterns[i];
            if(is_bad(pat)) {
                bad_patterns.push({
                    index: i,
                    pattern: pat.pattern,
                    rating: pat.stats.rating,
                });
            }
        }
        bad_patterns.sort((p1,p2) => p1.rating - p2.rating);
        bad_patterns = bad_patterns.slice(0, Math.min(30, bad_patterns.length));
        
        let groups = groupPatterns(bad_patterns, this.options);
        for(let group of groups) {
            if(group.length <= 1)
                continue;
            for(let i = 0; i < group.length; i++)
                group[i] = bad_patterns[group[i]].index;
            let new_pat = this.#unitePatterns(group);
            new_pat.stats.rating = calc_rating(new_pat.stats);
        }
    }

    #unitePatterns(group) {
        let new_pat;
        for(let idx of group) {
            let rec = this.patterns[idx];
            if(rec == null) {
                continue;
            }
            if(new_pat == null) {
                new_pat = {...rec, is_new: true};
                continue;
            }
            let res = mergeWithPattern(rec.pattern, new_pat.pattern, this.options);
            if(res == null)
                return;
            new_pat.pattern = res.pattern;
            appendToArray(new_pat.msg_indexes, rec.msg_indexes);
            new_pat.stats.checked = Math.max(rec.stats.checked, new_pat.stats.checked);
            new_pat.stats.hits = Math.max(rec.stats.hits, new_pat.stats.hits);
            new_pat.stats.matched += rec.stats.matched + 1;
        }
        if(!this.#acceptablePattern(new_pat))
            return;
        this.patterns.push(new_pat);
        for(let idx of group) {
            this.patterns[idx] = null;
        }
        return new_pat;
    }

    #acceptablePattern(rec) {
        let pattern = rec.corrected_pattern || rec.pattern;
        let const_ratio = 1 - countEquals(pattern, this.options.mask_char) / pattern.length;
        if(const_ratio < this.options.min_const_part_ratio)
            return false;
        // options.max_crosspattern_match
        return true;
    }

    #getChangesReport() {
        const rec_to_result = (rec) => {
            return {
                pattern: rec.pattern,
                msg_indexes: rec.msg_indexes,
            }
        }
        let result = {};
        result.added = this.patterns.filter(rec => (rec != null) && rec.is_new)
            .map(rec_to_result);
        result.updated = this.patterns.filter(rec => (rec != null) && rec.is_updated)
            .map(rec_to_result);
        return result;
    }

    #cleanupPatterns() {
        this.patterns = this.patterns.filter(p => p != null);
        for(let rec of this.patterns) {
            delete rec.msg_indexes;
            delete rec.is_new;
            delete rec.is_updated;
        }
    }

    #adaptLoadData(jsdata) {
        let patterns = jsdata.patterns.map(p => { return {...p} });
        if(jsdata.mask_char !== this.options.mask_char) {
            for(let pat of patterns) {
                pat.pattern = pat.pattern.replace(jsobj.mask_char, this.options.mask_char);
            }
        }
        return {...jsdata, patterns,
            mask_char: this.options.mask_char,
        };
    }
}

function closestPatternForMessage(patterns, msg, opt=null) {
    opt = {
        min_match_ratio: DEF_MIN_MATCH_RATIO,
        max_unique_match_ratio: DEF_MAX_UNIQUE_MATCH_RATIO,
        ratio_rel_tol: DEF_RATIO_REL_TOL,
        len_diff_max_ratio: DEF_LEN_DIFF_MAX_RATIO,
        ...opt
    }
    const { 
        min_match_ratio,
        max_unique_match_ratio,
        ratio_rel_tol,
        len_diff_max_ratio 
    } = opt;

    let index = -1;
    if(!opt?.no_fast_search_prepass) {
        index = fastSearchPattern(patterns, msg, opt);
        if(index !== -1) {
            for(let i = 0; i <= index; i++) {
                patterns[i].stats.checked++;
                patterns[i].stats.hits++;
            }
            return { index, ratio: 1.0, corrected_pattern: patterns[index].pattern }
        }
    }

    let ratio = min_match_ratio;
    let second_ratio = 0.0;
    let corrected_pattern = "";
    for(let i = 0; i < patterns.length; i++) {
        patterns[i].stats.checked++;
        let pattern = patterns[i].pattern;
        if(diffRatio(pattern.length, msg.length) > len_diff_max_ratio)
            continue;
        let lopt = optimalParameters(pattern, msg, opt);
        lopt.min_match_ratio = ratio * (1 - ratio_rel_tol);
        let result = alignAndMask(pattern, msg, lopt);
        if(result?.pattern == null || result.match_ratio < min_match_ratio)
            continue;
        patterns[i].stats.hits++;
        if(result.match_ratio > ratio) {
            ratio = result.match_ratio;
            index = i;
            corrected_pattern = result.pattern;
        } else {
            second_ratio = Math.max(result.match_ratio, second_ratio);
        }
    }
    if(ratio < max_unique_match_ratio)
        index = -1;
    if(index !== -1)
        return { index, ratio, corrected_pattern }
}

function fastSearchPattern(patterns, msg, opt=null) {
    let idx = patterns.findIndex(rec => rec.pattern === msg);
    if(idx !== -1)
        return idx;
    const len_diff_max_ratio = opt?.len_diff_max_ratio || DEF_LEN_DIFF_MAX_RATIO;
    const mask_char = opt?.mask_char || DEF_MASK_CHAR;
    idx = patterns.findIndex(rec => {
        let pattern = rec.pattern;
        if(diffRatio(pattern.length, msg.length) > len_diff_max_ratio)
            return false;
        if(pattern.includes(msg))
            return true;
        pattern = pattern.replace(mask_char, "");
        if(msg.includes(pattern))
            return true;
        return false;
    });
    return idx;
}

function groupPatterns(patterns, opt=null) {
    opt = {
        min_group_ratio: DEF_MIN_GROUP_RATIO,
        max_ratio_range: DEF_MAX_RATIO_RANGE,
        len_diff_max_ratio: DEF_LEN_DIFF_MAX_RATIO,
        ...opt
    }
    const { 
        min_group_ratio,
        max_ratio_range,
        len_diff_max_ratio 
    } = opt;

    function mergePatterns(p1, p2) {
        if(diffRatio(p1.length, p2.length) > len_diff_max_ratio)
            return null;
        let r = mergeWithPattern(p1, p2, opt);
        r.match_ratio = Math.max(r.match_ratio, r.back_match_ratio);
        r.match_ratio *= Math.sqrt(1 - diffRatio(p1.length, p2.length));
        return r.pattern ? r : null;
    }

    let matches = new Array(patterns.length);
    for(let j = 0; j < patterns.length; j++) {
        matches[j] = new Array(patterns.length);
        let p1 = patterns[j].pattern;
        matches[j][j] = { i:j, match:0.0 };
        for(let i = 0; i < j; i++) {
            let p2 = patterns[i].pattern;
            let match = mergePatterns(p1, p2)?.match_ratio || 0;
            matches[j][i] = { i, match };
            matches[i][j] = { i:j, match };
        }
    }
    for(let j = 0; j < matches.length; j++) {
        let mj = matches[j];
        if((mj?.length || 0) < 1) continue;
        mj.sort((b,a) => a.match - b.match);
        let biggest_match = mj[0].match;
        let min_match = Math.max(biggest_match - max_ratio_range, min_group_ratio);
        for(let i = mj.length - 1; i >= 0; i--) {
            if(mj[i].match >= min_match) {
                mj.length = i + 1;
                break;
            }
        }
    }

    let groups = [];
    const getMatches = (r) => (r != null) ? matches[r] : null;
    for(let row = 0; row < matches.length; row++) {
        let ml_row = getMatches(row);
        if(ml_row == null)
            continue;
        
        let group = [];
        function allNotInGroup(ml, end) {
            let first;
            let count = 0;
            end = Math.min(end, ml.length);
            for(let i = 0; i < end; i++)
                if(!group.includes(ml[i]?.i)) {
                    if(first == null)
                        first = ml[i]?.i;
                    count++;
                }
            return {first, count};
        }
        let curr = row;
        let ml_curr = ml_row;
        while(ml_curr) {
            let candidates = allNotInGroup(ml_curr, group.length + 1);
            if(candidates.count > 1)
                break;
            group.push(curr);
            if(candidates.count < 1)
                break;
            curr = candidates.first;
            ml_curr = getMatches(curr);
        }

        for(let gr_i of group) {
            matches[gr_i] = null;
        }
        groups.push(group);
    }

    return groups;
}

function mergeWithPattern(msg, pattern, opt=null) {
    if(msg === pattern)
        return {pattern, match_ratio: 1.0, back_match_ratio: 1.0};
    opt = optimalParameters(pattern, msg, opt);
    opt.min_group_ratio = opt.min_group_ratio || DEF_MIN_GROUP_RATIO;
    opt.ratio_rel_tol = opt.ratio_rel_tol || DEF_RATIO_REL_TOL;
    opt.min_match_ratio = opt.min_group_ratio * (1 - opt.ratio_rel_tol);
    return alignAndMask(pattern, msg, opt);
}

function optimalParameters(pattern, text, opt=null) {
    let maxdist = opt?.maxdist || 10;
    const mask_char = opt?.mask_char || DEF_MASK_CHAR;
    let num_mask = countEquals(pattern, mask_char, 0);
    if(num_mask > 0) {
        maxdist = Math.max(maxdist, Math.ceil(num_mask * 1.25));
        maxdist = Math.min(maxdist, Math.ceil(pattern.length * 0.5));
        maxdist = Math.max(maxdist, 5);
    } else {
        maxdist = Math.max(maxdist,
            Math.ceil(Math.max(pattern.length, text.length) * 0.075));
    }
    return { ...opt, mask_char, maxdist };
}

function alignAndMask(base, other, opt=null) {
    if(other === base)
        return {pattern: base, match_ratio: 1.0, back_match_ratio: 1.0};

    opt = {
        maxdist: DEF_MAX_DIST,
        min_match_ratio: DEF_MIN_MATCH_RATIO,
        sequence_minlen: DEF_SEQ_MIN_SIZE,
        mask_char: DEF_MASK_CHAR,
        ...opt
    };
    let { 
        maxdist, 
        min_match_ratio, 
        sequence_minlen, 
        mask_char 
    } = opt;
    maxdist = Math.min(maxdist, Math.max(base.length, other.length));

    function detectStableSequence(field, start, exclude_items = [0, FIELD_NONE]) {
        while(start + sequence_minlen - 1 < field.length) {
            let [pos, end] = firstMonotonusSequence(field, sequence_minlen, start);
            if(pos == null)
                break;
            start = end;
            let shift = field[pos];
            if(!exclude_items.includes(shift))
                return [pos, end, shift];
        }
        return [];
    }

    function calcMatchRatio(field) {
        let count = 0, start = 0;
        while(start < base.length) {
            let [pos, end] = detectStableSequence(field_bo, start, [FIELD_NONE]);
            if(pos == null) break;
            count += end - pos;
            start = end;
        }
        return count / field.length;
    }

    function shift(arr, pos, count) {
        arr.splice(pos, 0, ...(new Array(count)).fill(mask_char));
        return arr;
    }

    base = base.split('');
    other = other.split('');
    let field_bo = shiftMatchField(base, other, opt);
    let field_ob = shiftMatchField(other, base, opt);
    fieldsCorrections(field_bo, field_ob, base, other);

    let match_ratio = calcMatchRatio(field_bo);
    let num_mask = countEquals(base, mask_char, 0);
    let base_body_ratio = Math.max(0.15, 1 - num_mask / base.length);
    match_ratio /= base_body_ratio;

    let back_match_ratio = calcMatchRatio(field_ob);
    num_mask = countEquals(other, mask_char, 0);
    let other_body_ratio = Math.max(0.15, 1 - num_mask / other.length);
    back_match_ratio /= other_body_ratio;

    let max_match_ratio = Math.max(match_ratio, back_match_ratio);
    if(max_match_ratio < min_match_ratio)
        return { match_ratio, back_match_ratio };
    
    let start = 0;
    while(start < base.length) {
        let [pos, end, count] = detectStableSequence(field_bo, start);
        if(pos == null)
            break;
        if(count > 0) {
            shift(base, pos, count);
        } else if(count < 0) {
            pos += count;
            shift(other, pos, -count);
            count = 0;
        }
        start = end + count;

        field_bo = shiftMatchField(base, other, opt);
        field_ob = shiftMatchField(other, base, opt);
        fieldsCorrections(field_bo, field_ob, base, other);
    }

    match_ratio = countEquals(field_bo, 0, 0) / field_bo.length;
    match_ratio /= base_body_ratio;
    back_match_ratio = countEquals(field_ob, 0, 0) / field_ob.length;
    back_match_ratio /= other_body_ratio;

    max_match_ratio = Math.max(match_ratio, back_match_ratio);
    if(max_match_ratio < min_match_ratio)
        return { match_ratio, back_match_ratio };

    if(field_bo) {
        for(let i = 0; i < field_bo.length; i++) {
            if(field_bo[i] !== 0)
                base[i] = mask_char;
        }
    }

    return { pattern: base.join(''), match_ratio, back_match_ratio };
}

function fieldsCorrections(fa, fb, a, b) {
    function mutualShifts(fa, fb, apos) {
        return (fa[apos] !== FIELD_NONE) && (fb[apos + fa[apos]] !== FIELD_NONE)
        && (fa[apos] === -fb[apos + fa[apos]]);
    }

    function neighborCorrection(fa, fb, a, b, apos) {
        function checkNeighborTransfer(apos2) {
            return (apos2 > 0 && apos2 < fa.length
                && mutualShifts(fa, fb, apos2)
                && a[apos] === b[apos + fa[apos2]])
        }
        const left = checkNeighborTransfer(apos - 1);
        const rigth = checkNeighborTransfer(apos + 1);
        let new_pos;
        if(left && rigth && fa[apos + 1] === fa[apos - 1]) {
            new_pos = apos+1;
        }
        if(!mutualShifts(fa, fb, apos)) {
            if(left) new_pos = apos - 1;
            else if(rigth) new_pos = apos + 1;
        }
        if(new_pos) {
            fb[apos + fa[new_pos]] = -fa[new_pos];
            return fa[new_pos];
        }
        return fa[apos];
    }

    for(let i = 0; i < fa.length; i++) {
        fa[i] = neighborCorrection(fa, fb, a, b, i);
    }
    for(let i = 0; i < fb.length; i++) {
        fb[i] = neighborCorrection(fb, fa, b, a, i);
    }
    for(let i = 0; i < fa.length; i++) {
        if (!mutualShifts(fa, fb, i)) fa[i] = FIELD_NONE;
    }
}

function firstMonotonusSequence(array, minlen, start, end) {
    end = end || array.length;
    while(start + 1 < end) {
        while(array[start] !== array[start + 1] && start + 1 < end)
            start++;
        let pos = start + 1;
        while(array[pos] === array[pos + 1] && pos + 1 < end)
            pos++;
        pos++;
        if(pos - start >= minlen)
            return [start, pos];
        start = pos;
    }
    return [];
}

function shiftMatchField(base, other, opt) {
    let { maxdist, mask_char } = opt;
    let field = new Array(base.length);

    function searchMinimalShift(chari) {
        for(let shifti = 0; shifti < maxdist; shifti++) {
            if (chari + shifti < other.length 
            && base[chari] === other[chari + shifti]) {
                return shifti;
            }
            if (chari - shifti > -1 
            && base[chari] === other[chari - shifti]) {
                return -shifti;
            }
        }
        return FIELD_NONE;
    }

    for(let chari = 0; chari < base.length; chari++) {
        field[chari] = FIELD_NONE;
        if (base[chari] === "" || base[chari] === mask_char)
            continue;
        field[chari] = searchMinimalShift(chari);
    }
    return field;
}

function countEquals(array, value, start, end) {
    end = end || array.length;
    let count = 0;
    for(let i = start; i < end; i++)
        if(array[i] === value) count++;
    return count;
}

function appendToArray(dst, src) {
    let start = dst.length;
    dst.length += src.length;
    for(let i = 0; i < src.length; i++)
        dst[start + i] = src[i];
    return dst;
}

function diffRatio(a,b) {
    a = Math.abs(a); b = Math.abs(b);
    return Math.max(a,b) != 0.0 ? Math.abs(a - b) / Math.max(a,b) : 0.0;
}

if (!module.parent) {
    test();
  } else {
    module.exports = { Patterns }
  }