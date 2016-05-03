package com.github.seqware;

import java.util.function.Predicate;

import com.github.seqware.OxoGWrapperWorkflow.Pipeline;
import com.github.seqware.OxoGWrapperWorkflow.VCFType;

public interface CommonPredicates {

	static final Predicate<VcfInfo> isSanger = p -> p.getOriginatingPipeline() == Pipeline.sanger;
	static final Predicate<VcfInfo> isBroad = p -> p.getOriginatingPipeline() == Pipeline.broad;
	static final Predicate<VcfInfo> isDkfzEmbl = p -> p.getOriginatingPipeline() == Pipeline.dkfz_embl;
	static final Predicate<VcfInfo> isMuse = p -> p.getOriginatingPipeline() == Pipeline.muse;
	
	static final Predicate<VcfInfo> isIndel = p -> p.getVcfType() == VCFType.indel;
	static final Predicate<VcfInfo> isSnv = p -> p.getVcfType() == VCFType.snv;
	static final Predicate<VcfInfo> isSv = p -> p.getVcfType() == VCFType.sv;
}
