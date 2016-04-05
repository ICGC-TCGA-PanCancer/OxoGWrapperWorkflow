package com.github.seqware;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.hubspot.jinjava.Jinjava;

public class TemplateUtils {
	public static String getRenderedTemplate(String templateName ) {
		String renderedTemplate;
		String template = "";
		try {
			URI uri = TemplateUtils.class.getClassLoader().getResource(templateName).toURI();
			Path p = Paths.get(uri);
			template = new String (Files.readAllBytes(p));
		} catch (IOException | URISyntaxException e1) {
			e1.printStackTrace();
		}
		Jinjava jinjava = new Jinjava();
		Map<String, Object> context = new HashMap<String,Object>();
		renderedTemplate = jinjava.render(template, context );
		return renderedTemplate;
	}

	
	public static String getRenderedTemplate(Map<String, Object> context, String templateName ) {
		String renderedTemplate;
		String template = "";
		try {
			URI uri = TemplateUtils.class.getClassLoader().getResource(templateName).toURI();
			Path p = Paths.get(uri);
			template = new String (Files.readAllBytes(p));
		} catch (IOException | URISyntaxException e1) {
			e1.printStackTrace();
		}
		Jinjava jinjava = new Jinjava();
		renderedTemplate = jinjava.render(template, context);
		return renderedTemplate;
	}
	
	@SafeVarargs
	public static String getRenderedTemplate(String templateName, Entry<String, Object> ...entries)
	{
		Map<String,Object> context = new HashMap<String,Object>(entries.length);
		
		for (Entry<String,Object> e : entries)
		{
			context.entrySet().add(e);
		}
		
		return getRenderedTemplate(context,templateName);
	}
}
