function h = cl_vid( M )

frames = size(M,3);

colormap 'jet';

for i = 1:frames
	image(M(:,:,i));
	drawnow;
end

end