function [] = cl_summary(dir)

cldir = [dir '/clustering/'];
pngdir = [dir '/result/']; 
imdir = [dir '/src/'];

imfile = first_file(imdir);
fprintf("src: %s\n",imfile);

ismat = regexp(imfile,'.*(.mat)$');

if (ismat)
	disp('mat file');
	data = load(imfile);
	
	%I = data.I;
	%I = I ./ 4000;%(range(2)-range(1));
	
	I = -data.Z;
	%[range] = [min(min(I)) max(max(I))];
	% I = I .- range(1);
	% I = I ./ (range(2)-range(1));
	I = I ./ 2.3;%(range(2)-range(1));
	I = I .+ 1;
else
	disp('image file');
	I = imread (imfile);
	%[range] = [min(min(I)) max(max(I))]
	I = double(I) ./ 255.0;	
endif

[h w c] = size(I)

cldir = [dir '/clustering/'];
pngdir = [dir '/result/']; 
mkdir (dir,'/result/');
filelist = readdir(cldir);

for ii = 1:numel(filelist)
	
	clfile = filelist{ii};
	
	if (regexp (clfile, "^\\.\\.?$"))
		continue;
	endif
		
	C = cl_load ([cldir clfile]);
	
	if(ismat)
		M = cl_spat (C,[h w]);	
	else
		M = cl_spat (C,[w h]);
		M = permute (M,[2,1]);
	endif	
	
	CL = cl_viz (I,M);
	subplot(1,1,1);
	imshow(CL)
	pngfile = clfile;
	pngfile(pngfile==".") = "";
	
	if(ismat)
		imwrite(CL,[pngdir pngfile ".png"]);
	else
		imwrite(CL,[pngdir pngfile ".jpg"]);
	endif
	
	drawnow();
end

end