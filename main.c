/*
 *			GPAC - Multimedia Framework C SDK
 *
 *			Authors: Jean Le Feuvre, Cyril Concolato, Romain Bouqueau
 *			Copyright (c) Telecom ParisTech 2005-2012
 *					All rights reserved
 *
 *  This file is part of GPAC / mp4-to-ts (mp42ts) application
 *
 *  GPAC is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU Lesser General Public License as published by
 *  the Free Software Foundation; either version 2, or (at your option)
 *  any later version.
 *
 *  GPAC is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; see the file COPYING.  If not, write to
 *  the Free Software Foundation, 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 */

#include <gpac/media_tools.h>
#include <gpac/constants.h>
#include <gpac/base_coding.h>
#include <gpac/mpegts.h>

#ifndef GPAC_DISABLE_STREAMING
#include <gpac/internal/ietf_dev.h>
#endif

#ifndef GPAC_DISABLE_TTXT
#include <gpac/webvtt.h>
#endif

#ifdef GPAC_DISABLE_MPEG2TS_MUX
#error "Cannot compile MP42TS if GPAC is not built with MPEG2-TS Muxing support"
#endif

#define MP42TS_PRINT_TIME_MS 500 /*refresh printed info every CLOCK_REFRESH ms*/

static GFINLINE void usage()
{
	fprintf(stderr, "GPAC version " GPAC_FULL_VERSION "\n"
	        "GPAC Copyright (c) Telecom ParisTech 2000-2015\n"
	        "GPAC Configuration: " GPAC_CONFIGURATION "\n"
	        "Features: %s\n\n", gpac_features());
	fprintf(stderr, "mp42ts <inputs> <destinations> [options]\n"
	        "\n"
	        "Inputs:\n"
	        "-src filename[:OPTS]   specifies an input file used for a TS service\n"
	        "                        * currently only supports ISO files\n"
	        "                        * can be used several times, once for each source\n"
	        "By default each source is a program in a TS. \n"
	        "Source options are colon-separated list of options, as follows:\n"
	        "ID=N                   specifies the program ID for this source.\n"
	        "                       All sources with the same ID will be added to the same program\n"
	        "name=STR               program name, as used in DVB service description table\n"
	        "provider=STR           provider name, as used in DVB service description table\n"
	        "\n"
	        "Destinations:\n"
	        "Several destinations may be specified as follows, at least one is mandatory\n"
	        "-dst-file filename\n"
	        "The following parameters may be specified when -dst-file is used\n"
	        "-segment-dir dir       server local directory to store segments (ends with a '/')\n"
	        "-segment-duration dur  segment duration in seconds\n"
	        "-segment-manifest file m3u8 file basename\n"
	        "-segment-http-prefix p client address for accessing server segments\n"
	        "-segment-number n      number of segments to list in the manifest\n"
	        "\n"
	        "Basic options:\n"
	        "-rate R                specifies target rate in kbps of the multiplex (optional)\n"
	        "-real-time             specifies the muxer will work in real-time mode\n"
	        "                        * if not specified, the muxer will generate the TS as quickly as possible\n"
	        "-pcr-init V            sets initial value V for PCR - if not set, random value is used\n"
	        "-pcr-offset V          offsets all timestamps from PCR by V, in 90kHz. Default value is computed based on input media.\n"
	        "-psi-rate V            sets PSI refresh rate V in ms (default 100ms).\n"
	        "                        * If 0, PSI data is only send once at the beginning or before each IDR when -rap option is set.\n"
	        "                        * This should be set to 0 for DASH streams.\n"
	        "-single-au             forces 1 PES = 1 AU (disabled by default)\n"
	        "-rap                   forces RAP/IDR to be aligned with PES start for video streams (disabled by default)\n"
	        "                          in this mode, PAT, PMT and PCR will be inserted before the first TS packet of the RAP PES\n"
	        "-flush-rap             same as -rap but flushes all other streams (sends remaining PES packets) before inserting PAT/PMT\n"
	        "-pcr-ms N              sets max interval in ms between 2 PCR. Default is 100 ms\n"
	        "-ifce IPIFCE           specifies default IP interface to use. Default is IF_ANY.\n"
	        "\n"
	        "Misc options\n"
#ifdef GPAC_MEMORY_TRACKING
	        "-mem-track             enables memory tracker\n"
#endif
	        "-logs                  set log tools and levels, formatted as a ':'-separated list of toolX[:toolZ]@levelX\n"
	        "-h or -help            print this screen\n"
	        "\n"
	       );
}


#define MAX_MUX_SRC_PROG	100
typedef struct
{

#ifndef GPAC_DISABLE_ISOM
	GF_ISOFile *mp4;
#endif

	u32 nb_streams, pcr_idx;
	GF_ESInterface streams[40];
	GF_Thread *th;
	u32 rate;
	Bool audio_configured;
	u64 samples_done, samples_count;
	u32 nb_real_streams;
	Bool real_time;

	u32 max_sample_size;

	char program_name[20];
	char provider_name[20];
	u32 ID;
	Bool is_not_program_declaration;

	Double last_ntp;
} M2TSSource;

#ifndef GPAC_DISABLE_ISOM
typedef struct
{
	GF_ISOFile *mp4;
	u32 track, sample_number, sample_count;
	u32 mstype, mtype;
	GF_ISOSample *sample;
	/*refresh rate for images*/
	u32 image_repeat_ms, nb_repeat_last;
	void *dsi;
	u32 dsi_size;

	void *dsi_and_rap;
	Bool loop;
	Bool is_repeat;
	s64 ts_offset;
	M2TSSource *source;
} GF_ESIMP4;
#endif

typedef struct
{
	u32 ts_delta;
	u16 aggregate_on_stream;
	Bool discard;
	Bool rap;
	Bool critical;
	Bool vers_inc;
} GF_ESIStream;

#define VIDEO_DATA_ESID	105

#ifndef GPAC_DISABLE_ISOM

static GF_Err mp4_input_ctrl(GF_ESInterface *ifce, u32 act_type, void *param)
{
	GF_ESIMP4 *priv = (GF_ESIMP4 *)ifce->input_udta;
	if (!priv) return GF_BAD_PARAM;

	switch (act_type) {
	case GF_ESI_INPUT_DATA_FLUSH:
	{
		GF_ESIPacket pck;
#ifndef GPAC_DISABLE_TTXT
		GF_List *cues = NULL;
#endif
		if (!priv->sample)
			priv->sample = gf_isom_get_sample(priv->mp4, priv->track, priv->sample_number+1, NULL);

		if (!priv->sample) {
			return GF_IO_ERR;
		}

		memset(&pck, 0, sizeof(GF_ESIPacket));

		pck.flags = GF_ESI_DATA_AU_START | GF_ESI_DATA_HAS_CTS;
		if (priv->sample->IsRAP) pck.flags |= GF_ESI_DATA_AU_RAP;
		pck.cts = priv->sample->DTS + priv->ts_offset;
		if (priv->is_repeat) pck.flags |= GF_ESI_DATA_REPEAT;

		if (priv->nb_repeat_last) {
			pck.cts += priv->nb_repeat_last*ifce->timescale * priv->image_repeat_ms / 1000;
		}

		pck.dts = pck.cts;
		if (priv->sample->CTS_Offset) {
			pck.cts += priv->sample->CTS_Offset;
			pck.flags |= GF_ESI_DATA_HAS_DTS;
		}

		if (priv->sample->IsRAP && priv->dsi && priv->dsi_size) {
			pck.data = priv->dsi;
			pck.data_len = priv->dsi_size;
			ifce->output_ctrl(ifce, GF_ESI_OUTPUT_DATA_DISPATCH, &pck);
			pck.flags &= ~GF_ESI_DATA_AU_START;
		}

		pck.flags |= GF_ESI_DATA_AU_END;
		pck.data = priv->sample->data;
		pck.data_len = priv->sample->dataLength;
		pck.duration = gf_isom_get_sample_duration(priv->mp4, priv->track, priv->sample_number+1);
#ifndef GPAC_DISABLE_TTXT
		if (priv->mtype==GF_ISOM_MEDIA_TEXT && priv->mstype==GF_ISOM_SUBTYPE_WVTT) {
			u64             start;
			GF_WebVTTCue    *cue;
			GF_List *gf_webvtt_parse_iso_cues(GF_ISOSample *iso_sample, u64 start);
			start = (priv->sample->DTS * 1000) / ifce->timescale;
			cues = gf_webvtt_parse_iso_cues(priv->sample, start);
			if (gf_list_count(cues)>1) {
				GF_LOG(GF_LOG_DEBUG, GF_LOG_CONTAINER, ("[MPEG-2 TS Muxer] More than one cue in sample\n"));
			}
			cue = (GF_WebVTTCue *)gf_list_get(cues, 0);
			if (cue) {
				pck.data = cue->text;
				pck.data_len = (u32)strlen(cue->text)+1;
			} else {
				pck.data = NULL;
				pck.data_len = 0;
			}
		}
#endif
		ifce->output_ctrl(ifce, GF_ESI_OUTPUT_DATA_DISPATCH, &pck);
		GF_LOG(GF_LOG_DEBUG, GF_LOG_CONTAINER, ("[MPEG-2 TS Muxer] Track %d: sample %d CTS %d\n", priv->track, priv->sample_number+1, pck.cts));

#ifndef GPAC_DISABLE_TTXT
		if (cues) {
			while (gf_list_count(cues)) {
				GF_WebVTTCue *cue = (GF_WebVTTCue *)gf_list_get(cues, 0);
				gf_list_rem(cues, 0);
				gf_webvtt_cue_del(cue);
			}
			gf_list_del(cues);
			cues = NULL;
		}
#endif
		gf_isom_sample_del(&priv->sample);
		priv->sample_number++;

		if (!priv->source->real_time && !priv->is_repeat) {
			priv->source->samples_done++;
			gf_set_progress("Converting to MPEG-2 TS", priv->source->samples_done, priv->source->samples_count);
		}

		if (priv->sample_number==priv->sample_count) {
			if (priv->loop) {
				Double scale;
				u64 duration;
				/*increment ts offset*/
				scale = gf_isom_get_media_timescale(priv->mp4, priv->track);
				scale /= gf_isom_get_timescale(priv->mp4);
				duration = (u64) (gf_isom_get_duration(priv->mp4) * scale);
				priv->ts_offset += duration;
				priv->sample_number = 0;
				priv->is_repeat = (priv->sample_count==1) ? 1 : 0;
			}
			else if (priv->image_repeat_ms && priv->source->nb_real_streams) {
				priv->nb_repeat_last++;
				priv->sample_number--;
				priv->is_repeat = 1;
			} else {
				if (!(ifce->caps & GF_ESI_STREAM_IS_OVER)) {
					ifce->caps |= GF_ESI_STREAM_IS_OVER;
					if (priv->sample_count>1) {
						assert(priv->source->nb_real_streams);
						priv->source->nb_real_streams--;
					}
				}
			}
		}
	}
	return GF_OK;

	case GF_ESI_INPUT_DESTROY:
		if (priv->dsi) gf_free(priv->dsi);
		if (ifce->decoder_config) {
			gf_free(ifce->decoder_config);
			ifce->decoder_config = NULL;
		}
		gf_free(priv);
		ifce->input_udta = NULL;
		return GF_OK;
	default:
		return GF_BAD_PARAM;
	}
}

static void fill_isom_es_ifce(M2TSSource *source, GF_ESInterface *ifce, GF_ISOFile *mp4, u32 track_num, Bool compute_max_size)
{
	GF_ESIMP4 *priv;
	char *_lan;
	GF_ESD *esd;
	u64 avg_rate, duration;
	s32 ref_count;
	s64 mediaOffset;

	GF_SAFEALLOC(priv, GF_ESIMP4);

	priv->mp4 = mp4;
	priv->track = track_num;
	priv->mtype = gf_isom_get_media_type(priv->mp4, priv->track);
	priv->mstype = gf_isom_get_media_subtype(priv->mp4, priv->track, 1);
	priv->loop = source->real_time ? 1 : 0;
	priv->sample_count = gf_isom_get_sample_count(mp4, track_num);
	source->samples_count += priv->sample_count;
	if (priv->sample_count>1)
		source->nb_real_streams++;

	priv->source = source;
	memset(ifce, 0, sizeof(GF_ESInterface));
	ifce->stream_id = gf_isom_get_track_id(mp4, track_num);

	esd = gf_media_map_esd(mp4, track_num);

	if (esd) {
		ifce->stream_type = esd->decoderConfig->streamType;
		ifce->object_type_indication = esd->decoderConfig->objectTypeIndication;
		if (esd->decoderConfig->decoderSpecificInfo && esd->decoderConfig->decoderSpecificInfo->dataLength) {
			switch (esd->decoderConfig->objectTypeIndication) {
			case GPAC_OTI_AUDIO_AAC_MPEG4:
			case GPAC_OTI_AUDIO_AAC_MPEG2_MP:
			case GPAC_OTI_AUDIO_AAC_MPEG2_LCP:
			case GPAC_OTI_AUDIO_AAC_MPEG2_SSRP:
			case GPAC_OTI_VIDEO_MPEG4_PART2:
				ifce->decoder_config = (char *)gf_malloc(sizeof(char)*esd->decoderConfig->decoderSpecificInfo->dataLength);
				ifce->decoder_config_size = esd->decoderConfig->decoderSpecificInfo->dataLength;
				memcpy(ifce->decoder_config, esd->decoderConfig->decoderSpecificInfo->data, esd->decoderConfig->decoderSpecificInfo->dataLength);
				break;
			case GPAC_OTI_VIDEO_HEVC:
			case GPAC_OTI_VIDEO_SHVC:
			case GPAC_OTI_VIDEO_AVC:
			case GPAC_OTI_VIDEO_SVC:
				gf_isom_set_nalu_extract_mode(mp4, track_num, GF_ISOM_NALU_EXTRACT_LAYER_ONLY | GF_ISOM_NALU_EXTRACT_INBAND_PS_FLAG | GF_ISOM_NALU_EXTRACT_ANNEXB_FLAG | GF_ISOM_NALU_EXTRACT_VDRD_FLAG);
				break;
			case GPAC_OTI_SCENE_VTT_MP4:
				ifce->decoder_config = (char *)gf_malloc(sizeof(char)*esd->decoderConfig->decoderSpecificInfo->dataLength);
				ifce->decoder_config_size = esd->decoderConfig->decoderSpecificInfo->dataLength;
				memcpy(ifce->decoder_config, esd->decoderConfig->decoderSpecificInfo->data, esd->decoderConfig->decoderSpecificInfo->dataLength);
				break;
			}
		}
		gf_odf_desc_del((GF_Descriptor *)esd);
	}
	gf_isom_get_media_language(mp4, track_num, &_lan);
	if (!_lan || !strcmp(_lan, "und")) {
		ifce->lang = 0;
	} else {
		ifce->lang = GF_4CC(_lan[0],_lan[1],_lan[2],' ');
	}
	if (_lan) {
		gf_free(_lan);
	}

	ifce->timescale = gf_isom_get_media_timescale(mp4, track_num);
	ifce->duration = gf_isom_get_media_timescale(mp4, track_num);
	avg_rate = gf_isom_get_media_data_size(mp4, track_num);
	avg_rate *= ifce->timescale * 8;
	if (0!=(duration=gf_isom_get_media_duration(mp4, track_num)))
		avg_rate /= duration;

	if (gf_isom_has_time_offset(mp4, track_num)) ifce->caps |= GF_ESI_SIGNAL_DTS;

	ifce->bit_rate = (u32) avg_rate;
	ifce->duration = (Double) (s64) gf_isom_get_media_duration(mp4, track_num);
	ifce->duration /= ifce->timescale;

	GF_SAFEALLOC(ifce->sl_config, GF_SLConfig);
	ifce->sl_config->tag = GF_ODF_SLC_TAG;
//	ifce->sl_config->predefined = 3;
	ifce->sl_config->useAccessUnitStartFlag = 1;
	ifce->sl_config->useAccessUnitEndFlag = 1;
	ifce->sl_config->useRandomAccessPointFlag = 1;
	ifce->sl_config->useTimestampsFlag = 1;
	ifce->sl_config->timestampLength = 33;
	ifce->sl_config->timestampResolution = ifce->timescale;

	
#ifdef GPAC_DISABLE_ISOM_WRITE
	fprintf(stderr, "Warning: GPAC was compiled without ISOM Write support, can't set SL Config!\n");
#else
	gf_isom_set_extraction_slc(mp4, track_num, 1, ifce->sl_config);
#endif

	ifce->input_ctrl = mp4_input_ctrl;
	if (priv != ifce->input_udta) {
		if (ifce->input_udta)
			gf_free(ifce->input_udta);
		ifce->input_udta = priv;
	}


	if (! gf_isom_get_edit_list_type(mp4, track_num, &mediaOffset)) {
		priv->ts_offset = mediaOffset;
	}

	ref_count = gf_isom_get_reference_count(mp4, track_num, GF_ISOM_REF_SCAL);
	if (ref_count > 0) {
		gf_isom_get_reference_ID(mp4, track_num, GF_ISOM_REF_SCAL, (u32) ref_count, &ifce->depends_on_stream);
	}
	else {
		ifce->depends_on_stream = 0;
	}

	if (compute_max_size) {
		u32 i;
		for (i=0; i < priv->sample_count; i++) {
			u32 s = gf_isom_get_sample_size(mp4, track_num, i+1);
			if (s>source->max_sample_size) source->max_sample_size = s;
		}
	}

}

#endif //GPAC_DISABLE_ISOM

static volatile Bool run = 1;


static Bool open_source(M2TSSource *source, char *src, Bool force_real_time, Bool compute_max_size)
{
	s64 min_offset = 0;

	memset(source, 0, sizeof(M2TSSource));

	/*open ISO file*/
#ifndef GPAC_DISABLE_ISOM
	if (gf_isom_probe_file(src)) {
		u32 i;
		u32 nb_tracks;
		u32 first_audio = 0;
		u32 first_other = 0;
		source->mp4 = gf_isom_open(src, GF_ISOM_OPEN_READ, 0);
		source->nb_streams = 0;
		source->real_time = force_real_time;
		/*on MPEG-2 TS, carry 3GPP timed text as MPEG-4 Part17*/
		gf_isom_text_set_streaming_mode(source->mp4, 1);
		nb_tracks = gf_isom_get_track_count(source->mp4);

		for (i=0; i<nb_tracks; i++) {
			Bool check_deps = 0;
			if (gf_isom_get_media_type(source->mp4, i+1) == GF_ISOM_MEDIA_HINT)
				continue;

			fill_isom_es_ifce(source, &source->streams[i], source->mp4, i+1, compute_max_size);
			if (min_offset > ((GF_ESIMP4 *)source->streams[i].input_udta)->ts_offset)
				min_offset = ((GF_ESIMP4 *)source->streams[i].input_udta)->ts_offset;

			switch(source->streams[i].stream_type) {
			case GF_STREAM_VISUAL:
				/*turn on image repeat*/
				check_deps = 1;
				if (gf_isom_get_sample_count(source->mp4, i+1)>1) {
					/*get first visual stream as PCR*/
					if (!source->pcr_idx) 
						source->pcr_idx = i+1;
				}
				break;
			case GF_STREAM_AUDIO:
				if (!first_audio) first_audio = i+1;
				check_deps = 1;
				break;
			default:
				/*log not supported stream type: %s*/
				break;
			}
			source->nb_streams++;
			if (gf_isom_get_sample_count(source->mp4, i+1)>1) first_other = i+1;

			if (check_deps) {
				u32 k;
				Bool found_dep = 0;
				for (k=0; k<nb_tracks; k++) {
					if (gf_isom_get_media_type(source->mp4, k+1) != GF_ISOM_MEDIA_OD)
						continue;

					/*this stream is not refered to by any OD, send as regular PES*/
					if (gf_isom_has_track_reference(source->mp4, k+1, GF_ISOM_REF_OD, gf_isom_get_track_id(source->mp4, i+1) )==1) {
						found_dep = 1;
						break;
					}
				}
				if (!found_dep)
					source->streams[i].caps |= GF_ESI_STREAM_WITHOUT_MPEG4_SYSTEMS;
				
			}
		}

		/*if no visual PCR found, use first audio*/
		if (!source->pcr_idx) source->pcr_idx = first_audio;
		if (!source->pcr_idx) source->pcr_idx = first_other;
		if (source->pcr_idx) {
			GF_ESIMP4 *priv;
			source->pcr_idx-=1;
			priv = source->streams[source->pcr_idx].input_udta;
			gf_isom_set_default_sync_track(source->mp4, priv->track);
		}

		if (min_offset < 0) {
			for (i=0; i<source->nb_streams; i++) {
				((GF_ESIMP4 *)source->streams[i].input_udta)->ts_offset += -min_offset;
			}
		}
	}
	return 1;
}

#endif




#ifdef GPAC_MEMORY_TRACKING
static Bool enable_mem_tracker = GF_FALSE;
#endif

/*macro to keep retro compatibility with '=' and spaces in parse_args*/
#define CHECK_PARAM(param) (!strnicmp(arg, param, strlen(param)) \
        && (   ((arg[strlen(param)] == '=') && (next_arg = arg+strlen(param)+1)) \
            || ((strlen(arg) == strlen(param)) && ++i && (i<argc) && (next_arg = argv[i]))))

/*parse MP42TS arguments*/
static GFINLINE GF_Err parse_args(int argc, char **argv, u32 *mux_rate, s64 *pcr_init_val, u32 *pcr_offset, u32 *psi_refresh_rate, Bool *single_au_pes, M2TSSource *sources, u32 *nb_sources,
                                  Bool *real_time, char **ts_out, u16 *output_port,
                                  char** segment_dir, u32 *segment_duration, char **segment_manifest, u32 *segment_number, char **segment_http_prefix, u32 *split_rap, u32 *pcr_ms)
{
	Bool rate_found = GF_FALSE, dst_found = GF_FALSE, seg_dur_found = GF_FALSE, seg_dir_found = GF_FALSE, seg_manifest_found = GF_FALSE, seg_number_found = GF_FALSE, seg_http_found = GF_FALSE, real_time_found = GF_FALSE;
	char *arg = NULL, *next_arg = NULL, *error_msg = "no argument found";
	s32 i;

	/*first pass: find audio - NO GPAC INIT MODIFICATION MUST OCCUR IN THIS PASS*/
	for (i=1; i<argc; i++) {
		arg = argv[i];
		if (!stricmp(arg, "-h") || strstr(arg, "-help")) {
			usage();
			return GF_EOS;
		} else if (CHECK_PARAM("-pcr-init")) {
			sscanf(next_arg, LLD, pcr_init_val);
		} else if (CHECK_PARAM("-pcr-offset")) {
			*pcr_offset = atoi(next_arg);
		} else if (CHECK_PARAM("-psi-rate")) {
			*psi_refresh_rate = atoi(next_arg);
		} else if (!strcmp(arg, "-mem-track")) {
#ifdef GPAC_MEMORY_TRACKING
			gf_sys_close();
			enable_mem_tracker = GF_TRUE;
			gf_sys_init(GF_TRUE);
			gf_log_set_tool_level(GF_LOG_MEMORY, GF_LOG_INFO);
#else
			fprintf(stderr, "WARNING - GPAC not compiled with Memory Tracker - ignoring \"-mem-track\"\n");
#endif
		} else if (CHECK_PARAM("-rate")) {
			if (rate_found) {
				error_msg = "multiple '-rate' found";
				arg = NULL;
				goto error;
			}
			rate_found = 1;
			*mux_rate = 1000*atoi(next_arg);
		} else if (!strnicmp(arg, "-real-time", 10)) {
			if (real_time_found) {
				goto error;
			}
			real_time_found = 1;
			*real_time = 1;
		} else if (!stricmp(arg, "-single-au")) {
			*single_au_pes = 1;
		} else if (!stricmp(arg, "-rap")) {
			*split_rap = 1;
		} else if (!stricmp(arg, "-flush-rap")) {
			*split_rap = 2;
		} else if (CHECK_PARAM("-pcr-ms")) {
			*pcr_ms = atoi(next_arg);
		} else if (CHECK_PARAM("-logs")) {
			if (gf_log_set_tools_levels(next_arg) != GF_OK)
				return GF_BAD_PARAM;
		} else if (CHECK_PARAM("-segment-dir")) {
			if (seg_dir_found) {
				goto error;
			}
			seg_dir_found = 1;
			*segment_dir = next_arg;
			/* TODO: add the path separation char, if missing */
		} else if (CHECK_PARAM("-segment-duration")) {
			if (seg_dur_found) {
				goto error;
			}
			seg_dur_found = 1;
			*segment_duration = atoi(next_arg);
		} else if (CHECK_PARAM("-segment-manifest")) {
			if (seg_manifest_found) {
				goto error;
			}
			seg_manifest_found = 1;
			*segment_manifest = next_arg;
		} else if (CHECK_PARAM("-segment-http-prefix")) {
			if (seg_http_found) {
				goto error;
			}
			seg_http_found = 1;
			*segment_http_prefix = next_arg;
		} else if (CHECK_PARAM("-segment-number")) {
			if (seg_number_found) {
				goto error;
			}
			seg_number_found = 1;
			*segment_number = atoi(next_arg);
		} else if (CHECK_PARAM("-src")) {} //second pass arguments
		else if (CHECK_PARAM("-dst-file")) {
			dst_found = 1;
			*ts_out = gf_strdup(next_arg);
		} else {
			error_msg = "unknown option";
			goto error;
		}
	}
	rate_found = 1;

	/*second pass: other*/
	for (i=1; i<argc; i++) {
		u32 res;
		char *src_args;
		arg = argv[i];
		if (arg[0] !='-') continue;

		if (!CHECK_PARAM("-src")) continue;

		src_args = strchr(next_arg, ':');
		if (src_args && (src_args[1]=='\\')) {
			src_args = strchr(src_args+2, ':');
		}
		if (src_args) {
			src_args[0] = 0;
			src_args = src_args + 1;
		}

		res = open_source(&sources[*nb_sources], next_arg, *real_time, (*pcr_offset == (u32) -1) ? 1 : 0);

		//we may have arguments
		while (src_args) {
			char *sep = strchr(src_args, ':');
			if (sep) sep[0] = 0;

			if (!strnicmp(src_args, "name=", 5)) {
				strncpy(sources[*nb_sources].program_name, src_args+5, 20);
			} else if (!strnicmp(src_args, "provider=", 9)) {
				strncpy(sources[*nb_sources].provider_name, src_args+9, 20);
			} else if (!strnicmp(src_args, "ID=", 3)) {
				u32 k;
				sources[*nb_sources].ID = atoi(src_args+3);

				for (k=0; k<*nb_sources; k++) {
					if (sources[k].ID == sources[*nb_sources].ID) {
						sources[*nb_sources].is_not_program_declaration = 1;
						break;
					}
				}
			}

			if (sep) {
				sep[0] = ':';
				src_args = sep+1;
			} else
				break;
		}

		if (res) {
			(*nb_sources)++;
			if (res==2) *real_time=1;
		}
	}
	/*syntax is correct; now testing the presence of mandatory arguments*/
	if (dst_found && *nb_sources && rate_found) {
		return GF_OK;
	} else {
		if (!dst_found)
			fprintf(stderr, "Error: Destination argument not found\n");
		if (! *nb_sources)
			fprintf(stderr, "Error: No Programs are available\n");
		usage();
		return GF_BAD_PARAM;
	}

error:
	if (!arg) {
		fprintf(stderr, "Error: %s\n", error_msg);
	} else {
		fprintf(stderr, "Error: %s \"%s\"\n", error_msg, arg);
	}
	return GF_BAD_PARAM;
}

static GF_Err write_manifest(char *manifest, char *segment_dir, u32 segment_duration, char *segment_prefix, char *http_prefix, u32 first_segment, u32 last_segment, Bool end)
{
	#define arg_check(hey) hey == NULL ? "" : hey
	FILE *manifest_fp;
	u32 i;
	char manifest_tmp_name[GF_MAX_PATH];
	char manifest_name[GF_MAX_PATH];
	char *tmp_manifest = manifest_tmp_name;

	if (segment_dir) {
		sprintf(manifest_tmp_name, "%stmp.m3u8", segment_dir);
		sprintf(manifest_name, "%s%s", segment_dir, manifest);
	} else {
		sprintf(manifest_tmp_name, "tmp.m3u8");
		sprintf(manifest_name, "%s", manifest);
	}

	manifest_fp = gf_fopen(tmp_manifest, "w");
	if (!manifest_fp) {
		fprintf(stderr, "Could not create m3u8 manifest file (%s)\n", tmp_manifest);
		return GF_BAD_PARAM;
	}

	fprintf(manifest_fp, "#EXTM3U\n#EXT-X-TARGETDURATION:%u\n#EXT-X-MEDIA-SEQUENCE:%u\n", segment_duration, first_segment);

	for (i = first_segment; i <= last_segment; i++) 
		fprintf(manifest_fp, "#EXTINF:%u,\n%s%s_%u.ts\n", segment_duration, arg_check(http_prefix), segment_prefix, i);
	if (end) fprintf(manifest_fp, "#EXT-X-ENDLIST\n");
	gf_fclose(manifest_fp);

	if (!rename(tmp_manifest, manifest_name)) 
		return GF_OK;
	 else {
		if (remove(manifest_name)) {
			fprintf(stderr, "Error removing file %s\n", manifest_name);
			return GF_IO_ERR;
		} else if (rename(tmp_manifest, manifest_name)) {
			fprintf(stderr, "Could not rename temporary m3u8 manifest file (%s) into %s\n", tmp_manifest, manifest_name);
			return GF_IO_ERR;
		} else 
			return GF_OK;
		
	}
}

int main(int argc, char **argv)
{
	/********************/
	/*   declarations   */
	/********************/
	const char *ts_pck;
	char *ts_out              = NULL;
	char *segment_manifest    = NULL;
	char *segment_dir         = NULL;
	char *segment_http_prefix = NULL;
	u32 pcr_offset  = (u32) -1;
	u32 segment_number    = 10; 
	u32 pcr_ms            = 100;
	u32 split_rap         = 0;
	u32 mux_rate          = 0;
	u32 nb_sources        = 0;
	u32 segment_duration  = 0;
	u32 segment_index     = 0;
	u32 i, j, cur_pid, last_print_time, usec_till_next;
	u32 psi_refresh_rate = GF_M2TS_PSI_DEFAULT_REFRESH_RATE;
	Bool real_time     = GF_FALSE;
	Bool single_au_pes = GF_FALSE;
	Bool is_stdout     = GF_FALSE;
	s64 pcr_init_val = -1;
	FILE *ts_output_file = NULL;
	u16 output_port = 1234;
	M2TSSource sources[MAX_MUX_SRC_PROG];
	char segment_manifest_default[GF_MAX_PATH], segment_prefix[GF_MAX_PATH], segment_name[GF_MAX_PATH];
	GF_M2TS_Time prev_seg_time;
	GF_M2TS_Mux *muxer = NULL;

	/*****************/
	/*   gpac init   */
	/*****************/
	gf_sys_init(GF_FALSE);
	gf_log_set_tool_level(GF_LOG_ALL, GF_LOG_WARNING);

	/***********************/
	/*   initialisations   */
	/***********************/
	prev_seg_time.sec = 0;
	prev_seg_time.nanosec = 0;
	
	/***********************/
	/*   parse arguments   */
	/***********************/
	if (GF_OK != parse_args(argc, argv, &mux_rate, &pcr_init_val, &pcr_offset, &psi_refresh_rate, &single_au_pes, sources, &nb_sources,
	                        &real_time, &ts_out, &output_port,
	                        &segment_dir, &segment_duration, &segment_manifest, &segment_number, &segment_http_prefix, &split_rap, &pcr_ms)) {
		goto exit;
	}

	/***************************/
	/*   create mp42ts muxer   */
	/***************************/
	muxer = gf_m2ts_mux_new(mux_rate, psi_refresh_rate, real_time);
	if (muxer) gf_m2ts_mux_use_single_au_pes_mode(muxer, single_au_pes);
	if (pcr_init_val>=0) gf_m2ts_mux_set_initial_pcr(muxer, (u64) pcr_init_val);
	gf_m2ts_mux_set_pcr_max_interval(muxer, pcr_ms);

	if (ts_out != NULL) {
		if (segment_duration) {
			strcpy(segment_prefix, ts_out);
			if (segment_dir) {
				if (strchr("\\/", segment_name[strlen(segment_name)-1])) {
					sprintf(segment_name, "%s%s_%d.ts", segment_dir, segment_prefix, segment_index);
				} else {
					sprintf(segment_name, "%s/%s_%d.ts", segment_dir, segment_prefix, segment_index);
				}
			} else {
				sprintf(segment_name, "%s_%d.ts", segment_prefix, segment_index);
			}
			ts_out = gf_strdup(segment_name);
			if (!segment_manifest) {
				sprintf(segment_manifest_default, "%s.m3u8", segment_prefix);
				segment_manifest = segment_manifest_default;
			}
			//write_manifest(segment_manifest, segment_dir, segment_duration, segment_prefix, segment_http_prefix, segment_index, 0, 0);
		}
		if (!strcmp(ts_out, "stdout") || !strcmp(ts_out, "-") ) {
			ts_output_file = stdout;
			is_stdout = GF_TRUE;
		} else {
			ts_output_file = gf_fopen(ts_out, "wb");
			is_stdout = GF_FALSE;
		}
		if (!ts_output_file) {
			fprintf(stderr, "Error opening %s\n", ts_out);
			goto exit;
		}
	}

	if (nb_sources == 0) fprintf(stderr, "No program to mux, quitting.\n");

	for (i=0; i<nb_sources; i++) {
		if (!sources[i].ID) {
			for (j=i+1; j<nb_sources; j++) {
				if (sources[i].ID < sources[j].ID) sources[i].ID = sources[i].ID+1;
			}
			if (!sources[i].ID) sources[i].ID = 1;
		}
	}

	/****************************************/
	/*   declare all streams to the muxer   */
	/****************************************/
	cur_pid = 100;	/*PIDs start from 100*/
	for (i=0; i<nb_sources; i++) {
		GF_M2TS_Mux_Program *program;

		if (! sources[i].is_not_program_declaration) {
			u32 prog_pcr_offset = 0;
			if (pcr_offset==(u32)-1) {
				if (sources[i].max_sample_size && mux_rate) {
					Double r = sources[i].max_sample_size * 8;
					r *= 90000;
					r/= mux_rate;
					//add 10% of safety to cover TS signaling and other potential table update while sending the largest PES
					r *= 1.1;
					prog_pcr_offset = (u32) r;
				}
			} else {
				prog_pcr_offset = pcr_offset;
			}
			fprintf(stderr, "Setting up program ID %d - send rates: PSI %d ms PCR %d ms - PCR offset %d\n", sources[i].ID, psi_refresh_rate, pcr_ms, prog_pcr_offset);

			program = gf_m2ts_mux_program_add(muxer, sources[i].ID, cur_pid, psi_refresh_rate, prog_pcr_offset, GF_FALSE);
		} else {
			program = gf_m2ts_mux_program_find(muxer, sources[i].ID);
		}
		if (!program) continue;

		for (j=0; j<sources[i].nb_streams; j++) {
			GF_M2TS_Mux_Stream *stream;
			Bool force_pes_mode = 0;
			/*likely an OD stream disabled*/
			if (!sources[i].streams[j].stream_type) continue;

			stream = gf_m2ts_program_stream_add(program, &sources[i].streams[j], cur_pid+j+1, (sources[i].pcr_idx==j) ? 1 : 0, force_pes_mode);
			if (split_rap && (sources[i].streams[j].stream_type==GF_STREAM_VISUAL)) stream->start_pes_at_rap = 1;
		}

		cur_pid += sources[i].nb_streams;
		while (cur_pid % 10)
			cur_pid ++;

		if (sources[i].program_name[0] || sources[i].provider_name[0] ) gf_m2ts_mux_program_set_name(program, sources[i].program_name, sources[i].provider_name);
	}
	muxer->flush_pes_at_rap = (split_rap == 2) ? GF_TRUE : GF_FALSE;
	
	gf_m2ts_mux_update_config(muxer, 1);

	/*****************/
	/*   main loop   */
	/*****************/
	last_print_time = gf_sys_clock();
	while (run) {
		u32 status;

		/*flush all packets*/
		while ((ts_pck = gf_m2ts_mux_process(muxer, &status, &usec_till_next)) != NULL) {
			if (ts_output_file != NULL) {
				gf_fwrite(ts_pck, 1, 188, ts_output_file);
				if (segment_duration && (muxer->time.sec > prev_seg_time.sec + segment_duration)) {
					prev_seg_time = muxer->time;
					gf_fclose(ts_output_file);
					segment_index++;
					if (segment_dir) {
						if (strchr("\\/", segment_name[strlen(segment_name)-1])) {
							sprintf(segment_name, "%s%s_%d.ts", segment_dir, segment_prefix, segment_index);
						} else {
							sprintf(segment_name, "%s/%s_%d.ts", segment_dir, segment_prefix, segment_index);
						}
					} else {
						sprintf(segment_name, "%s_%d.ts", segment_prefix, segment_index);
					}
					ts_output_file = gf_fopen(segment_name, "wb");
					if (!ts_output_file) {
						fprintf(stderr, "Error opening %s\n", segment_name);
						goto exit;
					}
					/* delete the oldest segment */
					if (segment_number && ((s32) (segment_index - segment_number - 1) >= 0)) {
						char old_segment_name[GF_MAX_PATH];
						if (segment_dir) {
							if (strchr("\\/", segment_name[strlen(segment_name)-1])) {
								sprintf(old_segment_name, "%s%s_%d.ts", segment_dir, segment_prefix, segment_index - segment_number - 1);
							} else {
								sprintf(old_segment_name, "%s/%s_%d.ts", segment_dir, segment_prefix, segment_index - segment_number - 1);
							}
						} else {
							sprintf(old_segment_name, "%s_%d.ts", segment_prefix, segment_index - segment_number - 1);
						}
						gf_delete_file(old_segment_name);
					}
					write_manifest(segment_manifest, segment_dir, segment_duration, segment_prefix, segment_http_prefix,
//								   (segment_index >= segment_number/2 ? segment_index - segment_number/2 : 0), segment_index >1 ? segment_index-1 : 0, 0);
					               ( (segment_index > segment_number ) ? segment_index - segment_number : 0), segment_index >1 ? segment_index-1 : 0, 0);
				}
			}

			if (status>=GF_M2TS_STATE_PADDING)
				break;
			
		}

		if (real_time) {
			/*refresh every MP42TS_PRINT_TIME_MS ms*/
			u32 now=gf_sys_clock();
			if (now > last_print_time + MP42TS_PRINT_TIME_MS) {
				last_print_time = now;
				fprintf(stderr, "M2TS: time % 6d - TS time % 6d - avg bitrate % 8d\r", gf_m2ts_get_sys_clock(muxer), gf_m2ts_get_ts_clock(muxer), muxer->average_birate_kbps);

				if (gf_prompt_has_input()) {
					char c = gf_prompt_get_char();
					if (c=='q') break;
				}
			}
			if (status == GF_M2TS_STATE_IDLE) gf_sleep(1);
		}
		
		if (status==GF_M2TS_STATE_EOS) break;
	}

	{
		u64 bits = muxer->tot_pck_sent*8*188;
		u32 dur_sec = gf_m2ts_get_ts_clock(muxer) / 1000;
		if (!dur_sec) dur_sec = 1;
		fprintf(stderr, "Done muxing - %d sec - average rate %d kbps "LLD" packets written\n", dur_sec, (u32) (bits/dur_sec/1000), muxer->tot_pck_sent);
		fprintf(stderr, "\tPadding: "LLD" packets - "LLD" PES padded bytes (%g kbps)\n", muxer->tot_pad_sent, muxer->tot_pes_pad_bytes, (Double) (muxer->tot_pes_pad_bytes*8.0/dur_sec/1000) );
	}

exit:
	run = 0;
	if (segment_duration) {
		write_manifest(segment_manifest, segment_dir, segment_duration, segment_prefix, segment_http_prefix, segment_index - segment_number, segment_index, 1);
	}
	if (ts_output_file && !is_stdout) gf_fclose(ts_output_file);
	if (ts_out) gf_free(ts_out);

	if (muxer) gf_m2ts_mux_del(muxer);
	for (i=0; i<nb_sources; i++) {
		for (j=0; j<sources[i].nb_streams; j++) {
			if (sources[i].streams[j].input_ctrl) sources[i].streams[j].input_ctrl(&sources[i].streams[j], GF_ESI_INPUT_DESTROY, NULL);
			if (sources[i].streams[j].input_udta) {
				gf_free(sources[i].streams[j].input_udta);
			}
			if (sources[i].streams[j].decoder_config) {
				gf_free(sources[i].streams[j].decoder_config);
			}
			if (sources[i].streams[j].sl_config) {
				gf_free(sources[i].streams[j].sl_config);
			}
		}
#ifndef GPAC_DISABLE_ISOM
		if (sources[i].mp4) gf_isom_close(sources[i].mp4);
#endif

 		if (sources[i].th) gf_th_del(sources[i].th);
	}

	gf_sys_close();

#ifdef GPAC_MEMORY_TRACKING
	if (enable_mem_tracker && (gf_memory_size() || gf_file_handles_count() )) {
        gf_memory_print();
		return 2;
	}
#endif
	return 0;
}