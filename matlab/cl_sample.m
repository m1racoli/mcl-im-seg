function [] = cl_sample(dir)

imdir = [dir '/src/'];

imfile = first_file(imdir);
fprintf("src: %s\n",imfile);

ismat = regexp(imfile,'.*(.mat)$');

if (ismat)
	disp('mat file');
	data = load(imfile);
	
	I = data.I;
	I = I ./ 4000;
	imwrite(I,[dir "/sample_I.png"])
	
	I = -data.Z;
	[range] = [min(min(I)) max(max(I))];
	%I = I .- range(1);
	I = I ./ 2.3;%(range(2)-range(1));
	I = I .+ 1;
	imwrite(I,[dir "/sample_Z.png"])
else
	disp('image file');
	I = imread (imfile);
	imwrite(I,[dir "/sample.jpg"])
endif

end